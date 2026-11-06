//! Port of `packages/react/src/drawer/root/useDrawerSnapPoints.ts` (Base UI v1.9.0, MIT,
//! Copyright (c) 2019 Material-UI SAS; the licence text is in `swipe.rs`).
//!
//! The hook measures the viewport and the root font size, then resolves the configured snap
//! points against them in two `useMemo`s. Here the measurements are arguments and the memos are
//! functions.

use super::clamp;

/// `DrawerSnapPoint` (`drawer/root/DrawerRootContext.ts:7`): `number | string`. A number up to
/// `1` is a fraction of the viewport, a larger one is px; a string is `"<n>px"` or `"<n>rem"`.
#[derive(Clone, Debug, PartialEq)]
pub enum SnapPoint {
    Number(f64),
    Text(String),
}

/// `useDrawerSnapPoints.ts:11-15`.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedSnapPoint {
    pub value: SnapPoint,
    pub height: f64,
    pub offset: f64,
}

/// `useDrawerSnapPoints.ts:17-28`: resolves the vertical swipe movement for a snap point,
/// applying square-root damping once the drag overshoots the fully-open edge
/// (`next_offset < 0`) so the popup resists travelling past it.
pub fn get_snap_point_swipe_movement(base_offset: f64, movement_value: f64) -> f64 {
    let next_offset = base_offset + movement_value;
    if next_offset >= 0.0 {
        return movement_value;
    }
    -(-next_offset).sqrt() - base_offset
}

/// JavaScript's `parseFloat` as `resolveSnapPointValue` uses it: the longest numeric prefix of
/// the text, `NaN` when there is none. (Rust also reads `inf`/`nan` prefixes, which JavaScript
/// does not; every caller rejects non-finite values, so the difference is not observable.)
fn parse_float(text: &str) -> f64 {
    let text = text.trim_start();
    let mut end = text.len();
    loop {
        if text.is_char_boundary(end)
            && let Ok(value) = text[..end].parse::<f64>()
        {
            return value;
        }
        if end == 0 {
            return f64::NAN;
        }
        end -= 1;
    }
}

/// `useDrawerSnapPoints.ts:30-64`.
fn resolve_snap_point_value(
    snap_point: &SnapPoint,
    viewport_height: f64,
    root_font_size: f64,
) -> Option<f64> {
    if !viewport_height.is_finite() || viewport_height <= 0.0 {
        return None;
    }

    match snap_point {
        SnapPoint::Number(number) => {
            if !number.is_finite() {
                return None;
            }
            if *number <= 1.0 {
                return Some(clamp(*number, 0.0, 1.0) * viewport_height);
            }
            Some(*number)
        }
        SnapPoint::Text(text) => {
            let trimmed = text.trim();
            if trimmed.ends_with("px") {
                let value = parse_float(trimmed);
                return value.is_finite().then_some(value);
            }
            if trimmed.ends_with("rem") {
                let value = parse_float(trimmed);
                return value.is_finite().then_some(value * root_font_size);
            }
            None
        }
    }
}

/// `useDrawerSnapPoints.ts:66-82`: the index of the value closest to `target`, `None` (upstream
/// `-1`) if `values` is empty. The first of equally close values wins.
pub fn closest_snap_point_index(values: &[f64], target: f64) -> Option<usize> {
    let mut closest_index = None;
    let mut closest_distance = f64::INFINITY;
    for (index, value) in values.iter().enumerate() {
        let distance = (value - target).abs();
        if distance < closest_distance {
            closest_distance = distance;
            closest_index = Some(index);
        }
    }
    closest_index
}

/// `useDrawerSnapPoints.ts:118-161` (the `resolvedSnapPoints` memo). Points that do not resolve
/// are dropped, heights are clamped to the popup or the viewport, whichever is smaller, and of
/// points within 1px of each other the last one is kept.
pub fn resolve_snap_points(
    snap_points: &[SnapPoint],
    viewport_height: f64,
    root_font_size: f64,
    popup_height: f64,
) -> Vec<ResolvedSnapPoint> {
    if snap_points.is_empty() || viewport_height <= 0.0 || popup_height <= 0.0 {
        return vec![];
    }

    let max_height = popup_height.min(viewport_height);

    let resolved: Vec<ResolvedSnapPoint> = snap_points
        .iter()
        .filter_map(|value| {
            let resolved_height = resolve_snap_point_value(value, viewport_height, root_font_size)?;
            let clamped_height = clamp(resolved_height, 0.0, max_height);
            Some(ResolvedSnapPoint {
                value: value.clone(),
                height: clamped_height,
                offset: (popup_height - clamped_height).max(0.0),
            })
        })
        .collect();

    if resolved.len() <= 1 {
        return resolved;
    }

    let mut deduped: Vec<ResolvedSnapPoint> = Vec::with_capacity(resolved.len());
    for point in resolved.into_iter().rev() {
        let is_duplicate = deduped
            .iter()
            .any(|seen| (seen.height - point.height).abs() <= 1.0);
        if is_duplicate {
            continue;
        }
        deduped.push(point);
    }
    deduped.reverse();
    deduped
}

/// JavaScript's `Object.is` on two snap points (`NaN` equals `NaN`, `+0` is not `-0`).
fn object_is(left: &SnapPoint, right: &SnapPoint) -> bool {
    match (left, right) {
        (SnapPoint::Number(left), SnapPoint::Number(right)) => {
            (left.is_nan() && right.is_nan())
                || (left == right && left.is_sign_negative() == right.is_sign_negative())
        }
        (SnapPoint::Text(left), SnapPoint::Text(right)) => left == right,
        _ => false,
    }
}

/// `useDrawerSnapPoints.ts:163-186` (the `resolvedActiveSnapPoint` memo). `active_snap_point` is
/// `None` for upstream's `null` (closed). A value that is not one of the resolved points (it was
/// deduplicated away, say) lands on the resolved point nearest its own height.
pub fn resolve_active_snap_point<'a>(
    active_snap_point: Option<&SnapPoint>,
    resolved_snap_points: &'a [ResolvedSnapPoint],
    popup_height: f64,
    viewport_height: f64,
    root_font_size: f64,
) -> Option<&'a ResolvedSnapPoint> {
    let active_snap_point = active_snap_point?;

    if let Some(exact_match) = resolved_snap_points
        .iter()
        .find(|point| object_is(&point.value, active_snap_point))
    {
        return Some(exact_match);
    }

    let max_height = popup_height.min(viewport_height);
    let resolved_height =
        resolve_snap_point_value(active_snap_point, viewport_height, root_font_size)?;

    let clamped_height = clamp(resolved_height, 0.0, max_height);
    let heights: Vec<f64> = resolved_snap_points
        .iter()
        .map(|point| point.height)
        .collect();
    closest_snap_point_index(&heights, clamped_height).map(|index| &resolved_snap_points[index])
}

/// `activeSnapPointOffset: resolvedActiveSnapPoint?.offset ?? null` (`useDrawerSnapPoints.ts:195`).
pub fn active_snap_point_offset(
    resolved_active_snap_point: Option<&ResolvedSnapPoint>,
) -> Option<f64> {
    resolved_active_snap_point.map(|point| point.offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn number(value: f64) -> SnapPoint {
        SnapPoint::Number(value)
    }

    fn text(value: &str) -> SnapPoint {
        SnapPoint::Text(value.to_owned())
    }

    // Ports of `drawer/root/useDrawerSnapPoints.test.ts`.
    mod get_snap_point_swipe_movement {
        use super::super::get_snap_point_swipe_movement;

        #[test]
        fn returns_the_raw_movement_when_the_drag_does_not_overshoot_the_open_edge() {
            assert_eq!(get_snap_point_swipe_movement(100.0, -50.0), -50.0);
            assert_eq!(get_snap_point_swipe_movement(0.0, 20.0), 20.0);
        }

        #[test]
        fn returns_the_raw_movement_at_the_open_edge_next_offset_equals_0() {
            assert_eq!(get_snap_point_swipe_movement(100.0, -100.0), -100.0);
        }

        #[test]
        fn damps_the_movement_with_a_square_root_once_the_drag_overshoots_the_open_edge() {
            assert!((get_snap_point_swipe_movement(0.0, -150.0) + 150f64.sqrt()).abs() < 1e-9);
            assert!(
                (get_snap_point_swipe_movement(100.0, -250.0) - (-150f64.sqrt() - 100.0)).abs()
                    < 1e-9
            );
        }
    }

    mod closest_snap_point_index {
        use super::super::closest_snap_point_index;

        #[test]
        fn returns_the_closest_value_and_keeps_the_first_value_on_ties() {
            assert_eq!(
                closest_snap_point_index(&[100.0, 200.0, 300.0], 240.0),
                Some(1)
            );
            assert_eq!(closest_snap_point_index(&[100.0, 200.0], 150.0), Some(0));
        }

        #[test]
        fn returns_minus_1_when_there_are_no_values() {
            // Upstream's `-1` is `None`.
            assert_eq!(closest_snap_point_index(&[], 100.0), None);
        }
    }

    // Ports of the resolution assertions of `drawer/root/DrawerSnapPoints.test.tsx:53-133`,
    // against the same popup (300px) and viewport (400px) heights its `SnapPointsCase` sets.
    mod drawer_snap_point_composition {
        use super::*;

        const POPUP_HEIGHT: f64 = 300.0;
        const VIEWPORT_HEIGHT: f64 = 400.0;

        fn offset_of(
            snap_points: &[SnapPoint],
            active: Option<&SnapPoint>,
            root_font_size: f64,
        ) -> Option<f64> {
            let resolved =
                resolve_snap_points(snap_points, VIEWPORT_HEIGHT, root_font_size, POPUP_HEIGHT);
            active_snap_point_offset(resolve_active_snap_point(
                active,
                &resolved,
                POPUP_HEIGHT,
                VIEWPORT_HEIGHT,
                root_font_size,
            ))
        }

        #[test]
        fn resolves_supported_units_filters_invalid_values_clamps_heights_and_keeps_the_last_duplicate()
         {
            // The upstream test sets the root font size to 20px.
            let snap_points = [
                number(f64::NAN),
                text("invalid"),
                text("invalidpx"),
                text("invalidrem"),
                number(-1.0),
                text("2rem"),
                text("100px"),
                number(200.0),
                number(200.5),
                number(800.0),
            ];
            let resolved = resolve_snap_points(&snap_points, VIEWPORT_HEIGHT, 20.0, POPUP_HEIGHT);
            assert_eq!(
                resolved,
                vec![
                    ResolvedSnapPoint {
                        value: number(-1.0),
                        height: 0.0,
                        offset: 300.0
                    },
                    ResolvedSnapPoint {
                        value: text("2rem"),
                        height: 40.0,
                        offset: 260.0
                    },
                    ResolvedSnapPoint {
                        value: text("100px"),
                        height: 100.0,
                        offset: 200.0
                    },
                    ResolvedSnapPoint {
                        value: number(200.5),
                        height: 200.5,
                        offset: 99.5
                    },
                    ResolvedSnapPoint {
                        value: number(800.0),
                        height: 300.0,
                        offset: 0.0
                    },
                ]
            );
            assert_eq!(
                offset_of(&snap_points, Some(&number(200.0)), 20.0),
                Some(99.5)
            );
        }

        #[test]
        fn uses_fractional_and_pixel_snap_points_and_resolves_null_or_invalid_active_values_as_closed()
         {
            let snap_points = [number(0.5), number(200.0), text("2rem")];
            assert_eq!(
                offset_of(&snap_points, Some(&number(200.0)), 16.0),
                Some(100.0)
            );
            assert_eq!(offset_of(&snap_points, None, 16.0), None);
            assert_eq!(offset_of(&snap_points, Some(&text("invalid")), 16.0), None);
        }

        #[test]
        fn produces_no_resolved_snap_point_when_the_list_is_empty() {
            assert_eq!(
                resolve_snap_points(&[], VIEWPORT_HEIGHT, 16.0, POPUP_HEIGHT),
                vec![]
            );
            assert_eq!(offset_of(&[], None, 16.0), None);
        }

        #[test]
        fn resolves_a_single_snap_point_without_a_deduplication_pass() {
            let snap_points = [text("100px")];
            assert_eq!(
                resolve_snap_points(&snap_points, VIEWPORT_HEIGHT, 16.0, POPUP_HEIGHT),
                vec![ResolvedSnapPoint {
                    value: text("100px"),
                    height: 100.0,
                    offset: 200.0
                }]
            );
            assert_eq!(
                offset_of(&snap_points, Some(&text("100px")), 16.0),
                Some(200.0)
            );
        }
    }

    #[test]
    fn nothing_resolves_before_the_popup_and_the_viewport_are_measured() {
        // `useDrawerSnapPoints.ts:119`.
        let snap_points = [number(0.5)];
        assert_eq!(resolve_snap_points(&snap_points, 400.0, 16.0, 0.0), vec![]);
        assert_eq!(resolve_snap_points(&snap_points, 0.0, 16.0, 300.0), vec![]);
    }

    #[test]
    fn parse_float_reads_the_numeric_prefix_like_javascript() {
        assert_eq!(parse_float("100px"), 100.0);
        assert_eq!(parse_float("1.5rem"), 1.5);
        assert_eq!(parse_float("-2.5e1px"), -25.0);
        assert!(parse_float("px").is_nan());
        assert_eq!(parse_float("0x10px"), 0.0);
    }
}
