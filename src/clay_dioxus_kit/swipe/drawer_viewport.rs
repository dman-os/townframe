//! Port of the swipe arithmetic in `packages/react/src/drawer/viewport/DrawerViewport.tsx` (Base
//! UI v1.9.0, MIT, Copyright (c) 2019 Material-UI SAS; the licence text is in `swipe.rs`): the
//! release decision (dismiss, snap back, or settle on a snap point), the momentum projection and
//! the progress numbers that drive the backdrop and the parent drawers of a nest. Touch-scroll
//! arbitration, selection handling, controlled-mode rollback and style writes are DOM and are not
//! ported.
//!
//! Offsets are px of travel from fully open (`0`) towards closed (`popup_height`) for a `down`
//! drawer; an `up` drawer's snap points use the same positive offsets (see
//! [`super::drawer_popup::snap_point_offset_value`]).

use super::Size;
use super::clamp;
use super::sign;
use super::use_drawer_snap_points::ResolvedSnapPoint;
use super::use_drawer_snap_points::SnapPoint;
use super::use_drawer_snap_points::closest_snap_point_index;
use super::use_swipe_dismiss::ProgressDetails;
use super::use_swipe_dismiss::Release;
use super::use_swipe_dismiss::SwipeDirection;
use super::use_swipe_dismiss::get_displacement;

/// `DrawerViewport.tsx:45`.
pub const MIN_SWIPE_THRESHOLD: f64 = 10.0;
/// `DrawerViewport.tsx:46`.
pub const FAST_SWIPE_VELOCITY: f64 = 0.5;
/// `DrawerViewport.tsx:47`.
pub const SNAP_VELOCITY_THRESHOLD: f64 = 0.5;
/// `DrawerViewport.tsx:48`: px of momentum per px/ms of release velocity.
pub const SNAP_VELOCITY_MULTIPLIER: f64 = 300.0;
/// `DrawerViewport.tsx:49`.
pub const MAX_SNAP_VELOCITY: f64 = 4.0;
/// `DrawerViewport.tsx:50`.
pub const MIN_SWIPE_RELEASE_VELOCITY: f64 = 0.2;
/// `DrawerViewport.tsx:51`.
pub const MAX_SWIPE_RELEASE_VELOCITY: f64 = 4.0;
/// `DrawerViewport.tsx:52`.
pub const MIN_SWIPE_RELEASE_DURATION_MS: f64 = 80.0;
/// `DrawerViewport.tsx:53`.
pub const MAX_SWIPE_RELEASE_DURATION_MS: f64 = 360.0;
/// `DrawerViewport.tsx:54`.
pub const MIN_SWIPE_RELEASE_SCALAR: f64 = 0.1;
/// `DrawerViewport.tsx:55`.
pub const MAX_SWIPE_RELEASE_SCALAR: f64 = 1.0;
/// `DrawerViewport.tsx:56`. Used by the touch-scroll arbitration, which is not ported; kept so the
/// constants stay complete.
pub const AXIS_LOCK_SLOP: f64 = 6.0;
/// `DrawerViewport.tsx:57`. See [`AXIS_LOCK_SLOP`].
pub const AXIS_LOCK_BIAS: f64 = 2.0;

/// The snap-point state `DrawerViewport` reads from `useDrawerSnapPoints()` and the root context
/// (`DrawerViewport.tsx:98-109`).
#[derive(Clone, Copy, Debug)]
pub struct DrawerSnap<'a> {
    /// The configured `snapPoints`; empty means none (`snapPoints.length === 0`).
    pub snap_points: &'a [SnapPoint],
    /// [`super::use_drawer_snap_points::resolve_snap_points`] of them.
    pub resolved: &'a [ResolvedSnapPoint],
    /// [`super::use_drawer_snap_points::active_snap_point_offset`].
    pub active_offset: Option<f64>,
    pub popup_height: f64,
    pub snap_to_sequential_points: bool,
}

/// What the `onRelease` option of the drawer's `useSwipeDismiss` call decides
/// (`DrawerViewport.tsx:434-638`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrawerRelease {
    /// `return undefined`: the hook's own decision (distance past its threshold) stands.
    Default,
    /// `return false` with nothing else changed: the drawer returns to where it was.
    Stay,
    /// `return true`: dismiss in `direction`. When snap points are configured the caller first
    /// sets the active snap point to `null` and honours a cancel of that change
    /// (`DrawerViewport.tsx:563-567`).
    Close { direction: SwipeDirection },
    /// `setActiveSnapPoint(point.value)` then `return false`: settle on the resolved snap point at
    /// this index of [`DrawerSnap::resolved`].
    SettleOnSnapPoint(usize),
}

/// `DrawerViewport.tsx:1102-1104`.
pub fn get_base_swipe_size(popup: Size, direction: SwipeDirection) -> f64 {
    if matches!(direction, SwipeDirection::Left | SwipeDirection::Right) {
        popup.width
    } else {
        popup.height
    }
}

/// `DrawerViewport.tsx:1106-1108`: half the popup, but at least [`MIN_SWIPE_THRESHOLD`].
pub fn get_base_swipe_threshold(popup: Size, direction: SwipeDirection) -> f64 {
    (get_base_swipe_size(popup, direction) * 0.5).max(MIN_SWIPE_THRESHOLD)
}

/// `DrawerViewport.tsx:140-157`'s value: the offset of the first snap point and the distance to
/// the next one, which is the stretch over which the backdrop progress runs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SnapPointRange {
    pub min_offset: f64,
    pub range: f64,
}

/// `DrawerViewport.tsx:140-157` (`snapPointRange`).
pub fn snap_point_range(
    snap: &DrawerSnap<'_>,
    swipe_direction: SwipeDirection,
) -> Option<SnapPointRange> {
    if snap.snap_points.len() < 2
        || snap.resolved.len() < 2
        || !matches!(swipe_direction, SwipeDirection::Down | SwipeDirection::Up)
    {
        return None;
    }
    let mut offsets: Vec<f64> = snap.resolved.iter().map(|point| point.offset).collect();
    offsets.sort_by(f64::total_cmp);
    let min_offset = offsets[0];
    let next_offset = offsets[1];
    Some(SnapPointRange {
        min_offset,
        range: next_offset - min_offset,
    })
}

/// `DrawerViewport.tsx:159-165` (`snapPointProgress`).
pub fn snap_point_progress(
    range: Option<SnapPointRange>,
    active_snap_point_offset: Option<f64>,
) -> Option<f64> {
    let range = range?;
    let active_snap_point_offset = active_snap_point_offset?;
    Some(clamp(
        (active_snap_point_offset - range.min_offset) / range.range,
        0.0,
        1.0,
    ))
}

/// `DrawerViewport.tsx:167-177` (`swipeDirections`): with snap points a `down` drawer can be
/// dragged both ways (and an `up` one likewise), the dismiss direction first.
pub fn swipe_directions(
    has_snap_points: bool,
    swipe_direction: SwipeDirection,
) -> &'static [SwipeDirection] {
    match (has_snap_points, swipe_direction) {
        (true, SwipeDirection::Down) => &[SwipeDirection::Down, SwipeDirection::Up],
        (true, SwipeDirection::Up) => &[SwipeDirection::Up, SwipeDirection::Down],
        (_, SwipeDirection::Down) => &[SwipeDirection::Down],
        (_, SwipeDirection::Up) => &[SwipeDirection::Up],
        (_, SwipeDirection::Left) => &[SwipeDirection::Left],
        (_, SwipeDirection::Right) => &[SwipeDirection::Right],
    }
}

/// What `applySwipeProgress` computes before it writes (`DrawerViewport.tsx:201-203`, `:223`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SwipeProgress {
    /// This drawer's own progress: the backdrop's `--drawer-swipe-progress` and the provider's
    /// `swipeProgress`. Zero for a nested drawer, whose backdrop belongs to its parent.
    pub swipe_progress: f64,
    /// The progress reported to the parent drawer (`notifyParentSwipeProgressChange`); the parent
    /// finishes its nested swipe when it falls to zero (`:208-210`).
    pub nested_swipe_progress: f64,
    /// Whether the backdrop shows progress at all (`showProgress`).
    pub show_progress: bool,
}

/// `applySwipeProgress` (`DrawerViewport.tsx:199-234`), the numbers.
pub fn apply_swipe_progress(
    open: bool,
    nested: bool,
    resolved_progress: f64,
    should_track_progress: bool,
) -> SwipeProgress {
    let is_active = open && !nested && should_track_progress;
    let swipe_progress = if is_active { resolved_progress } else { 0.0 };
    let nested_swipe_progress = if open && should_track_progress {
        resolved_progress
    } else {
        0.0
    };
    SwipeProgress {
        swipe_progress,
        nested_swipe_progress,
        show_progress: is_active && swipe_progress > 0.0,
    }
}

/// `resolveSwipeRelease` (`DrawerViewport.tsx:236-293`): the scalar (`--drawer-swipe-strength`)
/// that shortens the exit transition of a drawer flung out by `direction`, from `0.1` (a hard
/// fling, 80ms) to `1` (a slow one, 360ms). `None` when the drawer is not flung (no size, already
/// out, or too slow).
pub fn resolve_swipe_release(
    popup: Size,
    direction: SwipeDirection,
    release: &Release,
    snap: &DrawerSnap<'_>,
) -> Option<f64> {
    let size = get_base_swipe_size(popup, direction);
    if size <= 0.0 {
        return None;
    }

    // The snap point base offset shifts the popup along the dismiss direction for both `down`
    // (+offset) and `up` (-offset), so it always adds to the directional translation.
    let snap_point_base_offset = if matches!(direction, SwipeDirection::Down | SwipeDirection::Up)
        && !snap.snap_points.is_empty()
    {
        snap.active_offset.unwrap_or(0.0)
    } else {
        0.0
    };
    let translation_along_direction =
        snap_point_base_offset + get_displacement(direction, release.delta_x, release.delta_y);
    let remaining_distance = (size - translation_along_direction).max(0.0);
    if remaining_distance <= 0.0 {
        return None;
    }

    let release_velocity = get_displacement(
        direction,
        release.release_velocity_x,
        release.release_velocity_y,
    );
    let directional_velocity = if release_velocity.abs() > 0.0 {
        release_velocity
    } else {
        get_displacement(direction, release.velocity_x, release.velocity_y)
    };
    if directional_velocity <= MIN_SWIPE_RELEASE_VELOCITY {
        return None;
    }

    let clamped_velocity = clamp(
        directional_velocity,
        MIN_SWIPE_RELEASE_VELOCITY,
        MAX_SWIPE_RELEASE_VELOCITY,
    );
    // The gesture hook supplies finite deltas and velocities. The guards above keep the
    // remaining distance and divisor positive, so the duration stays within [MIN, MAX] and the
    // resulting scalar within (0, 1].
    let duration_ms = clamp(
        remaining_distance / clamped_velocity,
        MIN_SWIPE_RELEASE_DURATION_MS,
        MAX_SWIPE_RELEASE_DURATION_MS,
    );
    let normalized_duration = (duration_ms - MIN_SWIPE_RELEASE_DURATION_MS)
        / (MAX_SWIPE_RELEASE_DURATION_MS - MIN_SWIPE_RELEASE_DURATION_MS);
    Some(
        MIN_SWIPE_RELEASE_SCALAR
            + normalized_duration * (MAX_SWIPE_RELEASE_SCALAR - MIN_SWIPE_RELEASE_SCALAR),
    )
}

/// `updateNestedSwipeActive` (`DrawerViewport.tsx:295-308`): whether a nested drawer's swipe
/// counts as under way, which is when it has travelled [`MIN_SWIPE_THRESHOLD`]. Returns the new
/// `nestedSwipeActiveRef`; a change to `true` is when the parent is told
/// (`notifyParentSwipingChange(true)`).
pub fn update_nested_swipe_active(
    nested_swipe_active: bool,
    details: Option<&ProgressDetails>,
    swipe_direction: SwipeDirection,
) -> bool {
    if nested_swipe_active {
        return true;
    }
    let Some(details) = details else {
        return false;
    };
    let direction = details.direction.unwrap_or(swipe_direction);
    let delta = get_displacement(direction, details.delta_x, details.delta_y);
    delta.abs() >= MIN_SWIPE_THRESHOLD
}

/// The progress half of `onProgress` (`DrawerViewport.tsx:410-424`): with two or more snap points
/// the hook's own `progress` (a fraction of the popup) is replaced by the position between the
/// first two snap points; at rest it is the active snap point's. `swiping` is the hook's
/// `swipingRef`. (The snap-point movement of the same callback, `:399-408`, is
/// [`super::use_drawer_snap_points::get_snap_point_swipe_movement`] of `details.delta_y`.)
pub fn on_progress(
    progress: f64,
    swiping: bool,
    details: Option<&ProgressDetails>,
    snap: &DrawerSnap<'_>,
    swipe_direction: SwipeDirection,
) -> f64 {
    let range = snap_point_range(snap, swipe_direction);
    let snap_point_progress = snap_point_progress(range, snap.active_offset);

    let mut resolved_progress = progress;
    if let Some(range) = range
        && snap.popup_height > 0.0
    {
        let base_offset = snap.active_offset.unwrap_or(range.min_offset);
        let offset_to_progress =
            |next_offset: f64| clamp((next_offset - range.min_offset) / range.range, 0.0, 1.0);

        // Outside a drag the hook still reports the last drag deltas, both after a release and
        // on a gesture that never started (a press inside `Drawer.Content`). Recomputing from
        // them would re-apply drag progress to a drawer that rests on its snap point.
        if swiping
            && let Some(details) = details
            && details.delta_y.is_finite()
        {
            resolved_progress =
                offset_to_progress(clamp(base_offset + details.delta_y, 0.0, snap.popup_height));
        } else if let Some(snap_point_progress) = snap_point_progress {
            resolved_progress = snap_point_progress;
        }
    }
    resolved_progress
}

/// `closeFromSnapPoints` (`DrawerViewport.tsx:556-571`). An unattributed gesture (a mostly
/// horizontal flick) may settle on a snap point but must not dismiss: `useSwipeDismiss` drops a
/// directionless dismissal, stranding the popup visually closed while `open` stays `true`.
fn close_from_snap_points(
    direction: Option<SwipeDirection>,
    swipe_direction: SwipeDirection,
    fallback_snap_point: usize,
) -> DrawerRelease {
    if direction.is_none() {
        return DrawerRelease::SettleOnSnapPoint(fallback_snap_point);
    }
    DrawerRelease::Close {
        direction: swipe_direction,
    }
}

/// What `onRelease` has worked out of the drag before it picks a snap point
/// (`DrawerViewport.tsx:517-540`).
struct Drag {
    direction: Option<SwipeDirection>,
    swipe_direction: SwipeDirection,
    drag_direction: f64,
    current_offset: f64,
    target_offset: f64,
    resolved_directional_velocity: f64,
}

/// The `snapToSequentialPoints` branch of `onRelease` (`DrawerViewport.tsx:573-618`): a flick
/// moves at most one snap point, to the adjacent one in the drag direction when the finger
/// stopped short of it.
fn sequential_snap_release(snap: &DrawerSnap<'_>, drag: &Drag) -> DrawerRelease {
    let Drag {
        direction,
        swipe_direction,
        drag_direction,
        current_offset,
        target_offset,
        resolved_directional_velocity,
    } = *drag;
    let mut ordered: Vec<usize> = (0..snap.resolved.len()).collect();
    // A stable sort, as `Array.prototype.sort` is.
    ordered.sort_by(|first, second| {
        snap.resolved[*first]
            .offset
            .total_cmp(&snap.resolved[*second].offset)
    });
    let ordered_offsets: Vec<f64> = ordered
        .iter()
        .map(|index| snap.resolved[*index].offset)
        .collect();
    let current_index = closest_snap_point_index(&ordered_offsets, current_offset)
        .expect("resolved snap points are non-empty");
    let mut target_snap_point = closest_snap_point_index(&ordered_offsets, target_offset)
        .expect("resolved snap points are non-empty");

    let velocity_direction = sign(resolved_directional_velocity);
    let should_advance = drag_direction != 0.0
        && velocity_direction != 0.0
        && velocity_direction == drag_direction
        && resolved_directional_velocity.abs() >= SNAP_VELOCITY_THRESHOLD;
    let mut effective_target_offset = target_offset;

    if should_advance {
        let adjacent_index = clamp(
            current_index as f64 + drag_direction,
            0.0,
            (ordered.len() - 1) as f64,
        ) as usize;
        if adjacent_index != current_index {
            let adjacent_offset = ordered_offsets[adjacent_index];
            let should_force_adjacent = if drag_direction > 0.0 {
                target_offset < adjacent_offset
            } else {
                target_offset > adjacent_offset
            };
            if should_force_adjacent {
                target_snap_point = adjacent_index;
                effective_target_offset = adjacent_offset;
            }
        } else if drag_direction > 0.0 {
            return close_from_snap_points(direction, swipe_direction, ordered[target_snap_point]);
        }
    }

    let close_distance = (effective_target_offset - snap.popup_height).abs();
    let snap_distance = (effective_target_offset - ordered_offsets[target_snap_point]).abs();
    if close_distance < snap_distance {
        return close_from_snap_points(direction, swipe_direction, ordered[target_snap_point]);
    }

    DrawerRelease::SettleOnSnapPoint(ordered[target_snap_point])
}

/// `onRelease` (`DrawerViewport.tsx:434-638`). `popup` is the popup's `offsetWidth` x
/// `offsetHeight`, `swipe_direction` the root's, `release` what the hook reported.
pub fn on_release(
    popup: Size,
    swipe_direction: SwipeDirection,
    release: &Release,
    snap: &DrawerSnap<'_>,
) -> DrawerRelease {
    let Release {
        direction,
        delta_x,
        delta_y,
        velocity_x,
        velocity_y,
        release_velocity_y,
        ..
    } = *release;

    if snap.snap_points.is_empty() {
        let Some(direction) = direction else {
            return DrawerRelease::Default;
        };

        let directional_delta = get_displacement(direction, delta_x, delta_y);
        if directional_delta <= 0.0 {
            return DrawerRelease::Stay;
        }

        if get_displacement(direction, velocity_x, velocity_y) >= FAST_SWIPE_VELOCITY {
            return DrawerRelease::Close { direction };
        }

        return if directional_delta > get_base_swipe_threshold(popup, direction) {
            DrawerRelease::Close { direction }
        } else {
            DrawerRelease::Stay
        };
    }

    if !matches!(swipe_direction, SwipeDirection::Down | SwipeDirection::Up) {
        return DrawerRelease::Default;
    }

    let popup_height = snap.popup_height;
    if popup_height == 0.0 || popup_height.is_nan() {
        return DrawerRelease::Stay;
    }

    if snap.resolved.is_empty() {
        return DrawerRelease::Default;
    }

    let is_down = swipe_direction == SwipeDirection::Down;
    let drag_delta = if is_down { delta_y } else { -delta_y };
    let drag_direction = sign(drag_delta);
    let release_directional_velocity = if is_down {
        release_velocity_y
    } else {
        -release_velocity_y
    };
    let fallback_directional_velocity = if is_down { velocity_y } else { -velocity_y };
    let mut resolved_directional_velocity = release_directional_velocity;
    if drag_direction != 0.0 && drag_delta.abs() >= MIN_SWIPE_THRESHOLD {
        let velocity_direction = sign(resolved_directional_velocity);
        if velocity_direction != 0.0 && velocity_direction != drag_direction {
            // Ignore touch reversals that would otherwise flip the snap decision.
            resolved_directional_velocity = fallback_directional_velocity;
        }
    }

    let current_offset = snap.active_offset.unwrap_or(0.0);
    let drag_target_offset = clamp(current_offset + drag_delta, 0.0, popup_height);
    let velocity_offset = if resolved_directional_velocity.abs() >= SNAP_VELOCITY_THRESHOLD {
        clamp(
            resolved_directional_velocity,
            -MAX_SNAP_VELOCITY,
            MAX_SNAP_VELOCITY,
        ) * SNAP_VELOCITY_MULTIPLIER
    } else {
        0.0
    };
    let target_offset = if snap.snap_to_sequential_points {
        drag_target_offset
    } else {
        clamp(drag_target_offset + velocity_offset, 0.0, popup_height)
    };

    if snap.snap_to_sequential_points {
        return sequential_snap_release(
            snap,
            &Drag {
                direction,
                swipe_direction,
                drag_direction,
                current_offset,
                target_offset,
                resolved_directional_velocity,
            },
        );
    }

    let offsets: Vec<f64> = snap.resolved.iter().map(|point| point.offset).collect();
    let closest_snap_point = closest_snap_point_index(&offsets, target_offset)
        .expect("resolved snap points are non-empty");

    if resolved_directional_velocity >= FAST_SWIPE_VELOCITY && drag_delta > 0.0 {
        return close_from_snap_points(direction, swipe_direction, closest_snap_point);
    }

    let close_distance = (target_offset - popup_height).abs();
    if close_distance < (target_offset - offsets[closest_snap_point]).abs() {
        return close_from_snap_points(direction, swipe_direction, closest_snap_point);
    }

    DrawerRelease::SettleOnSnapPoint(closest_snap_point)
}

#[cfg(test)]
mod tests;
