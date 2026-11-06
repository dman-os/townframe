//! Port of the swipe arithmetic in `packages/react/src/drawer/popup/DrawerPopup.tsx` (Base UI
//! v1.9.0, MIT, Copyright (c) 2019 Material-UI SAS; the licence text is in `swipe.rs`). The rest
//! of that file (measuring, focus, data attributes, CSS variable registration) is DOM and is not
//! ported.

use super::use_swipe_dismiss::SwipeDirection;

/// `DrawerPopup.tsx:327-332`: the `--drawer-snap-point-offset` value in px. Snap points apply to
/// `down` and `up` drawers only; an `up` drawer's resting offset points the other way.
/// `has_snap_points` is `snapPoints && snapPoints.length > 0`.
pub fn snap_point_offset_value(
    has_snap_points: bool,
    swipe_direction: SwipeDirection,
    active_snap_point_offset: Option<f64>,
) -> Option<f64> {
    let should_apply_snap_points =
        has_snap_points && matches!(swipe_direction, SwipeDirection::Down | SwipeDirection::Up);
    let offset = active_snap_point_offset.filter(|_| should_apply_snap_points)?;
    Some(if swipe_direction == SwipeDirection::Up {
        -offset
    } else {
        offset
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snap_point_offset_value_follows_the_dismiss_direction() {
        let table = [
            (true, SwipeDirection::Down, Some(120.0), Some(120.0)),
            (true, SwipeDirection::Up, Some(120.0), Some(-120.0)),
            (true, SwipeDirection::Left, Some(120.0), None),
            (true, SwipeDirection::Right, Some(120.0), None),
            (false, SwipeDirection::Down, Some(120.0), None),
            (true, SwipeDirection::Down, None, None),
        ];
        for (has_snap_points, direction, offset, expected) in table {
            assert_eq!(
                snap_point_offset_value(has_snap_points, direction, offset),
                expected,
                "{has_snap_points} {direction:?} {offset:?}"
            );
        }
    }
}
