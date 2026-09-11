//! Arc index — a Niagara-style "smart scroll strip". Visual experiment.
//!
//! At rest this is an ordinary vertical index pinned to the right edge, sized so
//! the whole alphabet fits on screen at once. Pressing it bends the strip
//! *around your thumb*: each label is pushed away from the edge, and the size of
//! that push falls off with the label's distance from the touch point. Labels
//! under the thumb move furthest; distant ones barely move.
//!
//! Niagara fires haptics each time a breakpoint is crossed. Desktop has no
//! haptic channel, so the bubble under the thumb is the place to pulse instead.
//!
//! # A displacement field, not an arc
//!
//! The tempting model is a circle centred on the thumb, but a circle *rotates*
//! the strip: every label moves in both x and y, by an amount set by its index
//! offset from the focus. Shrinking the radius then swings the whole strip
//! around the thumb and the ends wrap with it.
//!
//! What the strip actually does is displace sideways only:
//!
//! ```text
//! d     = distance from the thumb, in px
//! t     = falloff(d)                  1 at the thumb, 0 at the reach
//! limit = thumb intrusion + clearance, bounded by the far edge of the window
//! push  = floor + (limit - floor)*t
//! x     = rest_x + push               y is set by the list, untouched
//! ```
//!
//! The arc is then the *envelope* of that field rather than an imposed shape,
//! which is why it reads as a soft ribbon dented by a finger.
//!
//! The other end of that interpolation is `floor`, where every breakpoint out
//! of reach sits. It starts on the resting column and stays there while the gap
//! to the picked breakpoint is under `MAX_DIFF`, so a short drag dents the strip
//! without shifting it at all. Past that the floor starts to move, which is what
//! brings the far end along on a long drag — and the gap itself never widens
//! past the cap. `reach_for` grows with the drag, so a longer pull spreads the
//! dent over more breakpoints instead of only deepening it.
//!
//! # Vertical: only the ends move
//!
//! The list does *not* follow the thumb vertically. Inside the index the anchor
//! is exactly the resting position of the focused breakpoint, so the whole index
//! stays on screen and every breakpoint stays reachable — dragging up from the
//! middle moves the selection without shifting the strip, which is what stops A
//! escaping off the top.
//!
//! The strip only travels when a drag goes past ★ or #, and then it follows the
//! finger exactly: you are dragging the ends, not the strip.
//!
//! Both axes resolve from the pointer's *absolute* position, never from how far
//! the gesture has travelled. Measuring the pull from where the gesture began
//! while the selection was absolute was the bug that made the ends feel wrong:
//! the two disagreed, so the picked breakpoint slid out from under the thumb.
//!
//! # Deliberately web-portable
//!
//!   * absolutely positioned labels and no CSS `transform` (Blitz implements
//!     none, and this works unchanged in a browser),
//!   * the viewport arrives as a prop, so nothing here touches the renderer,
//!   * one `settle_tick` shim is the only platform-specific sleep.
//!
//! Blitz does not implement pointer capture yet — it is a 1.0 item, see
//! <https://github.com/DioxusLabs/blitz/issues/119> — so once a gesture starts
//! the *whole overlay* starts taking pointer input. Every move then lands on us
//! however far the finger travels from the edge, which is what lets the strip
//! follow a thumb that has left the grab band.

use dioxus::prelude::*;
use std::time::Duration;

// -- geometry knobs (tuned visually) -----------------------------------------

/// Fraction of the window height the resting index spans. This is what decides
/// the spacing: small enough to read as a dense index rather than a full-height
/// list, large enough that the full index still fits without scrolling.
const SPAN_FRAC: f64 = 0.52;
/// The breakpoint count `SPAN_FRAC` is calibrated against.
const REFERENCE_COUNT: usize = 28;
/// How far the resting column sits from the right edge.
const REST_INSET: f64 = 18.0;
/// How far past the thumb the label under it clears it by.
const PUSH_CLEARANCE: f64 = 44.0;
/// The largest gap allowed between the breakpoint under the thumb and the
/// breakpoints out of its reach.
///
/// This is the whole X model's one cap. While the gap is under it the far
/// breakpoints stay on the resting column, so a short drag leaves the strip
/// alone and only dents it. Once the gap reaches it the floor starts to move,
/// which is what brings the far end along on a long drag.
const MAX_DIFF: f64 = 400.0;
/// How far along the strip the dent reaches at the very start of a drag, in px,
/// and how much further it reaches per px of movement.
///
/// The reach grows with the drag, so pulling the thumb further across spreads
/// the dent over more breakpoints rather than just deepening it.
const REACH_BASE: f64 = 90.0;
const REACH_RATIO: f64 = 0.7;
/// Width of the band that starts a gesture. Only this strip takes input while
/// the strip is at rest, so the tiles underneath stay pressable.
const DRAG_BAND_WIDTH: f64 = 150.0;
/// Settle animation: how long it takes to unbend, and how many frames.
const SETTLE_MS: u64 = 260;
const SETTLE_STEPS: u32 = 16;
/// Used until the window has been measured.
const FALLBACK_HEIGHT: f64 = 860.0;

/// Breakpoints. Niagara uses a star for "favourites" then an A–Z index.
fn breakpoints() -> Vec<&'static str> {
    let mut labels = vec!["★"];
    labels.extend([
        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R",
        "S", "T", "U", "V", "W", "X", "Y", "Z",
    ]);
    labels.push("#");
    labels
}

/// Distance between neighbouring breakpoints, in px.
///
/// A function of the window alone, deliberately *not* of the breakpoint count:
/// the index is not stretched to fit its contents. Ten breakpoints make a short
/// strip sitting where the first ten of a full one would, rather than the same
/// strip spread out — so a breakpoint keeps its identity wherever it is.
fn spacing(height: f64) -> f64 {
    height * SPAN_FRAC / (REFERENCE_COUNT - 1) as f64
}

/// Where the resting index starts, so it is centred in the window.
fn top_inset(spacing: f64, count: usize, height: f64) -> f64 {
    let span = count.saturating_sub(1) as f64 * spacing;
    (height - span) / 2.0
}

/// Which breakpoint sits under a point in the resting layout.
///
/// The resting list is canonical — breakpoint `i` at `top + i * spacing`,
/// whatever the focus — so this is the inverse of that, and pressing a
/// breakpoint anchors the strip with the list exactly where it already was.
fn focus_at(y: f64, top: f64, spacing: f64, count: usize) -> f64 {
    ((y - top) / spacing).clamp(0.0, count.saturating_sub(1) as f64)
}
/// How far a point is past the first or last breakpoint, in px.
///
/// Zero anywhere inside the index, which is what keeps the list from moving
/// while a drag is selecting. Only a pull past an end is non-zero.
fn overscroll_at(y: f64, top: f64, spacing: f64, count: usize) -> f64 {
    let raw = (y - top) / spacing;
    (raw - focus_at(y, top, spacing, count)) * spacing
}

/// The pull the strip takes for a thumb at `y`.
///
/// The overshoot past an end, bounded by the slack at that end so the index can
/// never be dragged out of the window even if the pointer leaves it. Because the
/// resting index is centred, the slack at each end is exactly the furthest the
/// thumb can overshoot past it, so that bound is not reached in normal use.
fn pull_for(y: f64, top: f64, spacing: f64, count: usize, height: f64) -> f64 {
    let span = count.saturating_sub(1) as f64 * spacing;
    overscroll_at(y, top, spacing, count).clamp(-top, height - top - span)
}

/// Where the strip's anchor sits.
///
/// The resting position of the focused breakpoint, shifted by however far the
/// strip has been pulled past an end. The shift does not depend on the focus, so
/// neither do the labels' screen positions: inside the index the list is still
/// and only the selection moves.
///
/// Combined with a focus resolved from the same pointer, this puts the picked
/// breakpoint exactly at the thumb whether it is inside the index or past an
/// end.
fn anchor_y(focus: f64, spacing: f64, top: f64, pull: f64) -> f64 {
    top + focus * spacing + pull
}

/// Falloff from the thumb: 1 directly under it, 0 beyond the reach, with a soft
/// shoulder. Smoothstep rather than a linear ramp so the dent has no crease.
fn falloff(distance: f64, reach: f64) -> f64 {
    let t = (distance / reach).clamp(0.0, 1.0);
    1.0 - t * t * (3.0 - 2.0 * t)
}

/// How far along the strip the dent reaches, in px. Grows with the drag.
fn reach_for(limit: f64) -> f64 {
    REACH_BASE + REACH_RATIO * limit
}

/// The push the breakpoints out of the thumb's reach sit at.
///
/// Sits on the resting column until the gap to the breakpoint under the thumb
/// would exceed [`MAX_DIFF`], and only then moves with it. So a short drag dents
/// the strip without shifting it, and a long drag brings the far end along with
/// a gap that never widens past the cap.
fn push_floor(limit: f64) -> f64 {
    (limit - MAX_DIFF).max(0.0)
}

/// How far the label under the thumb travels: it keeps following the finger
/// until the labels run out of window.
///
/// `room` is the distance between the resting column and the far edge of the
/// window, so this is bounded by the *window*, not by a constant — the strip
/// follows the thumb all the way across rather than stopping part way.
fn push_limit(thumb_inboard: f64, room: f64) -> f64 {
    (thumb_inboard + PUSH_CLEARANCE).min(room).max(0.0)
}

/// How far a label `distance` px from the thumb is pushed from the edge.
fn push_at(distance: f64, floor: f64, limit: f64, reach: f64) -> f64 {
    floor + (limit - floor).max(0.0) * falloff(distance, reach)
}

/// One placed breakpoint. Offsets are from the strip's column and its anchor.
struct Placed {
    label: &'static str,
    /// How far this label is pushed left of the resting column, in px.
    push: f64,
    /// Vertical offset from the strip's anchor. Positive is down.
    dy: f64,
}

/// Project the breakpoint list onto the strip.
///
/// `settle` is 1 while a finger is down and eases to 0 once it lifts, so every
/// push is scaled by how engaged the strip currently is. `dy` is unaffected:
/// vertical position belongs to the list, and the anchor it hangs off is what
/// moves when the strip follows a thumb.
fn place(
    items: &[&'static str],
    focus: f64,
    spacing: f64,
    floor: f64,
    limit: f64,
    settle: f64,
) -> Vec<Placed> {
    let reach = reach_for(limit);
    items
        .iter()
        .enumerate()
        .map(|(index, label)| {
            let offset = index as f64 - focus;
            let distance = offset.abs() * spacing;
            Placed {
                label,
                push: push_at(distance, floor, limit, reach) * settle,
                dy: offset * spacing,
            }
        })
        .collect()
}

/// One frame-ish delay of the settle animation. The only platform-specific
/// shim here: a web target would use `requestAnimationFrame` instead.
async fn settle_tick() {
    tokio::time::sleep(Duration::from_millis(SETTLE_MS / SETTLE_STEPS as u64)).await;
}

#[component]
/// `viewport` is the window's inner size in logical px, supplied by the platform
/// shell.
///
/// It is a prop rather than a measurement because a Blitz node has no layout to
/// measure at mount time: the overlay's scroll size reads 0x0 then, and
/// `get_client_rect` additionally panics (`doc_mut` while borrowed). Handing in
/// plain numbers keeps this component free of any renderer dependency.
pub fn ArcIndex(viewport: (f64, f64)) -> Element {
    let items = use_signal(breakpoints);
    let count = items.read().len();

    // Continuous position in breakpoint space. The fractional part drives the
    // in-between spacing so the strip glides instead of stepping.
    let mut focus = use_signal(|| 6.0f64);
    // 1 while the strip is bent around a thumb, easing to 0 once it lifts.
    let mut settle = use_signal(|| 0.0f64);
    // Where the thumb is horizontally while pressed. Its vertical position needs
    // no state at all: the selection follows the pointer directly.
    let mut thumb_x = use_signal(|| None::<f64>);
    // How far past either end the pointer is, in px. Absolute, like the
    // selection, so the two cannot disagree.
    let mut pull = use_signal(|| 0.0f64);
    // Bumped on every press so a settle animation in flight can tell that it has
    // been superseded and stop writing.
    let mut generation = use_signal(|| 0u64);
    let mut announced = use_signal(|| 6usize);
    let mut live = use_signal(String::new);

    let active = (*focus.read()).round().clamp(0.0, (count - 1) as f64) as usize;
    // A zero-sized window happens before the shell reports a real size; fall
    // back so the rest layout is still sane rather than NaN-ridden.
    let (width, height) = if viewport.1 > 0.0 { viewport } else { (viewport.0, FALLBACK_HEIGHT) };
    let spacing = spacing(height);
    let top = top_inset(spacing, count, height);

    // `use_callback` so the same closure can be handed to several handlers.
    let commit = use_callback(move |next: f64| {
        let clamped = next.clamp(0.0, (count - 1) as f64);
        focus.set(clamped);

        let index = clamped.round() as usize;
        if index != *announced.peek() {
            announced.set(index);
            live.set(items.read()[index].to_string());
        }
    });

    let press_strip = use_callback(move |point: (f64, f64)| {
        // Start from the breakpoint actually under the finger, and leave the list
        // exactly where it is: only a drag past an end moves it.
        commit.call(focus_at(point.1, top, spacing, count));

        let next = *generation.peek() + 1;
        generation.set(next);
        thumb_x.set(Some(point.0));
        pull.set(pull_for(point.1, top, spacing, count, height));
        settle.set(1.0);
    });

    // Releasing eases the strip back to its resting column *and* back to its
    // resting position in the window, so the index is always fully visible once
    // you let go. `press` is held until the animation lands, because the strip
    // is anchored to the thumb for the whole of it.
    let release_strip = use_callback(move |()| {
        let mine = *generation.peek() + 1;
        generation.set(mine);

        spawn(async move {
            for step in 1..=SETTLE_STEPS {
                settle_tick().await;
                if *generation.peek() != mine {
                    return;
                }
                let progress = step as f64 / SETTLE_STEPS as f64;
                // Ease out: leaves the finger quickly, arrives slowly.
                let remaining = 1.0 - progress;
                settle.set(remaining * remaining * remaining);
            }
            if *generation.peek() == mine {
                settle.set(0.0);
                pull.set(0.0);
                thumb_x.set(None);
            }
        });
    });

    let settle_now = *settle.read();
    // How far the thumb has come in from the resting column, and how much window
    // is left for a label to retreat into.
    let room = (width - REST_INSET - spacing / 2.0).max(0.0);
    let inboard = (*thumb_x.read()).map_or(0.0, |x| (width - x - REST_INSET).max(0.0));
    // The breakpoint under the thumb, then everything out of reach behind it.
    let limit = push_limit(inboard, room);
    let floor = push_floor(limit);
    let reach = reach_for(limit);

    // Scaled by `settle` so the pull decays back to nothing on release.
    let anchor = anchor_y(*focus.read(), spacing, top, *pull.read() * settle_now);

    let placed = place(&items.read(), *focus.read(), spacing, floor, limit, settle_now);
    let active_label = items.read()[active];
    let active_dy = (active as f64 - *focus.read()) * spacing;

    // Cells tile the spacing exactly, so labels can never overlap however tight
    // the window makes them.
    let cell = spacing;
    // Kept well under the cell size: the labels are a positional index, not
    // something to read across the room.
    let font = (cell * 0.78).min(15.0);
    let bubble = cell * 0.95;
    // Inert while at rest so the tiles underneath stay clickable; once a
    // gesture starts the overlay takes the whole window, which is how the strip
    // keeps tracking a finger that has left the grab band.
    let pointer_events = if settle_now > 0.0 { "auto" } else { "none" };
    // The scrim only exists while the strip is bent into the content.
    let scrim = format!(
        "position:absolute; top:0; right:0; bottom:0; width:{}px; opacity:{settle_now}; \
         background:linear-gradient(to left, rgba(9,9,11,0.92), rgba(9,9,11,0)); pointer-events:none;",
        REST_INSET + limit + cell * 2.0 + 90.0
    );

    rsx! {
        div {
            class: "arc-index",
            // Inert while at rest so the tiles underneath stay clickable; once a
            // gesture starts it takes the whole window, which is how the strip
            // keeps tracking a finger that has left the grab band.
            style: "position:absolute; top:0; left:0; right:0; bottom:0; user-select:none; \
                    pointer-events:{pointer_events};",
            onpointermove: move |event| {
                // No pointerdown yet, so there is no gesture to continue.
                if thumb_x.peek().is_none() {
                    return;
                }
                // Page coordinates, not element ones: the event target switches
                // between the grab band and the overlay as the pointer crosses the
                // band's edge, and element coordinates are relative to whichever
                // one was hit. Page space is one frame for the whole gesture.
                let point = event.data().page_coordinates();
                // Both from the same absolute pointer, so the picked breakpoint
                // stays exactly under the thumb at every point of the drag.
                commit.call(focus_at(point.y, top, spacing, count));
                pull.set(pull_for(point.y, top, spacing, count, height));
                thumb_x.set(Some(point.x));
            },
            onpointerup: move |_| release_strip.call(()),
            onpointercancel: move |_| release_strip.call(()),
            onwheel: move |event| {
                let delta = event.data().delta().strip_units();
                let travel = if delta.y.abs() > delta.x.abs() { delta.y } else { delta.x };
                if travel != 0.0 {
                    commit.call(*focus.peek() + travel / spacing);
                }
            },

            div { style: "{scrim}" }

            // Every label carries a fixed-size flex cell because there is no
            // `transform: translate(-50%, -50%)` to centre with.
            for item in placed.iter() {
                span {
                    key: "{item.label}",
                    "aria-hidden": "true",
                    style: "position:absolute; right:{REST_INSET + item.push - cell / 2.0}px; \
                            top:{anchor + item.dy - cell / 2.0}px; width:{cell}px; height:{cell}px; \
                            display:flex; align-items:center; justify-content:center; \
                            font-size:{font}px; font-weight:600; color:#f3f4f6; \
                            pointer-events:none; \
                            text-shadow:0 1px 4px rgba(0,0,0,0.95), 0 0 2px rgba(0,0,0,0.95);",
                    "{item.label}"
                }
            }

            // Focus bubble, left of the strip, i.e. furthest from the thumb.
            div {
                "aria-hidden": "true",
                style: "position:absolute; \
                        right:{REST_INSET + push_at(active_dy.abs(), floor, limit, reach) * settle_now + cell * 0.6}px; \
                        top:{anchor + active_dy - bubble}px; width:{bubble * 2.0}px; height:{bubble * 2.0}px; \
                        border-radius:50%; background:rgba(120,120,128,0.45); \
                        display:flex; align-items:center; justify-content:center; \
                        font-size:{font * 1.3}px; color:#f3f4f6; pointer-events:none;",
                "{active_label}"
            }

            // Grab band. Pointer input starts here rather than on the labels:
            // without pointer capture we want one stable target for the whole
            // gesture, and it lets a drag start anywhere along the edge.
            div {
                style: "position:absolute; top:0; right:0; bottom:0; width:{DRAG_BAND_WIDTH}px; \
                        pointer-events:auto; touch-action:none;",
                onpointerdown: move |event| {
                    let point = event.data().page_coordinates();
                    press_strip.call((point.x, point.y));
                },
            }

            // Accessible surface: one slider whose value is the breakpoint
            // index, plus a live region so crossing a breakpoint is spoken.
            div {
                role: "slider",
                "aria-label": "index",
                "aria-valuemin": 1,
                "aria-valuemax": count,
                "aria-valuenow": active + 1,
                "aria-valuetext": "{active_label}",
                style: "position:absolute; width:1px; height:1px; overflow:hidden;",
                onkeydown: move |event| {
                    let step = match event.data().key() {
                        Key::ArrowDown | Key::ArrowRight | Key::PageDown => 1.0,
                        Key::ArrowUp | Key::ArrowLeft | Key::PageUp => -1.0,
                        Key::Home => -(count as f64),
                        Key::End => count as f64,
                        _ => 0.0,
                    };
                    if step != 0.0 {
                        commit.call((*focus.peek() + step).round());
                    }
                },
            }
            div {
                "aria-live": "polite",
                style: "position:absolute; width:1px; height:1px; overflow:hidden;",
                "{live}"
            }
        }
    }
}

#[cfg(test)]
mod tests {
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

        assert!(placed.iter().all(|p| p.push == 0.0), "idle list must not bow");

        let steps: Vec<f64> = placed.windows(2).map(|w| w[1].dy - w[0].dy).collect();
        assert!(steps.windows(2).all(|w| (w[0] - w[1]).abs() < 1e-9), "uneven: {steps:?}");
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
            assert_eq!(push_at(distance, floor, limit, reach), floor, "moved at {distance}");
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
            assert_eq!(push_floor(limit), 0.0, "far end moved at a limit of {limit}");
        }

        // Past the cap it comes along, and the gap stops growing.
        for limit in [MAX_DIFF + 1.0, MAX_DIFF + 150.0, 5000.0] {
            let floor = push_floor(limit);
            assert_eq!(floor, limit - MAX_DIFF, "far end did not follow at {limit}");
            assert_eq!(limit - floor, MAX_DIFF, "gap was not capped at {limit}");
            assert!(floor < limit, "the picked breakpoint must still move furthest");
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
        let anchor = anchor_y(l, spacing, top, pull_for(press_y, top, spacing, count, HEIGHT));
        assert!((anchor - press_y).abs() < 1e-9, "press moved the list to {anchor}");

        // Presses outside the index clamp rather than running off the ends.
        assert_eq!(focus_at(-500.0, top, spacing, count), 0.0);
        assert_eq!(focus_at(HEIGHT + 500.0, top, spacing, count), (count - 1) as f64);
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
            let anchor = anchor_y(focus, spacing, top, pull_for(y, top, spacing, count, HEIGHT));
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
            let anchor = anchor_y(focus, spacing, top, pull_for(y, top, spacing, count, HEIGHT));

            let first = anchor - focus * spacing;
            let last = first + span;
            assert!(first >= -1e-9, "top left the window with the thumb at {y}: {first}");
            assert!(last <= HEIGHT + 1e-9, "bottom left the window with the thumb at {y}");
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
}
