//! `ScrubStrip` (`ct-scrub-strip`): a Niagara-style index strip you scrub to jump.
//!
//! At rest this is an ordinary vertical index pinned to the right edge, sized so
//! the whole alphabet fits on screen at once. Pressing it bends the strip
//! *around your thumb*: each label is pushed away from the edge, and the size of
//! that push falls off with the label's distance from the touch point. Labels
//! under the thumb move furthest; distant ones barely move. (The "arc" the strip
//! draws while pressed is only that pressed look.)
//!
//! Niagara fires haptics each time a breakpoint is crossed. Desktop has no
//! haptic channel, so the bubble under the thumb is the place to pulse instead.
//!
//! # The contract
//!
//! Rust computes where everything goes and the stylesheet (`styles/kit/scrub_strip.css`) decides
//! what it looks like (`docs/design-language/styling-contract.md`).
//!
//! * Root `ct-scrub-strip`, `data-state`: `idle`, `hover` (a pointer is over the strip) or
//!   `engaged` (pressed). Inline custom properties: `--ct-strip-cell` (the size of one label's
//!   cell), `--ct-strip-inset` (the resting column's distance from the right edge),
//!   `--ct-strip-anchor` (the y of the focused label's centre) and `--ct-strip-limit` (how far
//!   the label under the thumb is pushed, `0px` at rest).
//! * Parts: `scrim`, `marker` (one per item; `data-state` `active` or `inactive`, `data-hover` on
//!   the one under a hovering pointer; custom properties `--ct-strip-push`, how far it is pushed
//!   left of the column, and `--ct-strip-dy`, its y offset from the anchor; it holds a `tick`
//!   and a `label`), `bubble` (while engaged: the active label, with the same two properties),
//!   `grab-band` (the pointer and keyboard target, `role="slider"`) and `status` (a live region).
//!
//! Releasing sets every property to its resting value at once; the stylesheet's `transition`
//! unbends the strip. There is no timer and no animation state here.
//!
//! # Keyboard and assistive technology
//!
//! The grab band is one slider whose value is the breakpoint (`aria-valuenow` is 1-based,
//! `aria-valuetext` the label). With keyboard focus: Down, Right and Page Down step to the next
//! breakpoint, Up, Left and Page Up to the previous, Home and End jump to the ends. A live region
//! speaks the breakpoint a pointer gesture lands on, since pressing does not focus the slider.
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
//! # Renderer limits
//!
//! * The strip measures the box its overlay covers (`position: fixed; inset: 0`) with
//!   `onmounted` and `onresize`, and reads pointer positions relative to that box, so a host
//!   never tells it how big the window is. A Blitz node has no layout to measure at mount time
//!   and its `get_client_rect` panics ([`Renderer::measures_layout`]), so there the box stays
//!   zero-sized: the strip still picks, but its labels do not travel inboard.
//! * Blitz does not implement pointer capture (a 1.0 item,
//!   <https://github.com/DioxusLabs/blitz/issues/119>), so once a gesture starts the *whole
//!   overlay* takes pointer input (`data-state="engaged"` turns its `pointer-events` on). Every
//!   move then lands on the strip however far the finger travels from the edge.
//! * Blitz panics on `tabindex` (`KIT-052`), so the slider is a tab stop only where
//!   [`Renderer::takes_focus`]; elsewhere it is pointer-only.

use super::sections::Renderer;
use dioxus::html::geometry::ClientPoint;
use dioxus::html::geometry::Pixels;
use dioxus::html::geometry::PixelsRect;
use dioxus::html::geometry::euclid::Point2D;
use dioxus::prelude::*;
use std::rc::Rc;

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
/// Extra spacing applied while the strip is actively engaged.
const ACTIVE_SPACING_MULTIPLIER: f64 = 1.35;
/// Used until the overlay has been measured.
const FALLBACK_HEIGHT: f64 = 860.0;

/// Breakpoints. Niagara uses a star for "favourites" then an A–Z index.
#[cfg(test)]
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
/// A function of the window alone up to [`REFERENCE_COUNT`] breakpoints,
/// deliberately *not* of how many there are: the index is not stretched to fit
/// its contents. Ten breakpoints make a short strip sitting where the first ten
/// of a full one would, rather than the same strip spread out — so a breakpoint
/// keeps its identity wherever it is.
///
/// Past the reference count the strip would no longer fit the window (and the
/// pull's slack would go negative), so the spacing shrinks to keep the whole
/// index spanning [`SPAN_FRAC`] of it: "the whole index fits on screen at once"
/// holds for any list, at the cost of tighter labels.
fn spacing(height: f64, count: usize) -> f64 {
    height * SPAN_FRAC / (count.max(REFERENCE_COUNT) - 1) as f64
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
/// While `engaged` (a finger is down) every label is pushed by the displacement field; at rest
/// none is. `dy` is the same either way: vertical position belongs to the list, and the anchor it
/// hangs off is what moves when the strip follows a thumb. The stylesheet animates the change
/// between the two with a transition, so there is no intermediate state to compute.
fn place(
    items: &[&'static str],
    focus: f64,
    spacing: f64,
    floor: f64,
    limit: f64,
    engaged: bool,
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
                push: if engaged {
                    push_at(distance, floor, limit, reach)
                } else {
                    0.0
                },
                dy: offset * spacing,
            }
        })
        .collect()
}

/// Everything the visual tree needs, derived once from the state and the viewport: the numbers
/// the stylesheet reads as custom properties.
struct Geometry {
    active: usize,
    active_label: &'static str,
    /// The y of the focused label's centre, in px.
    anchor: f64,
    /// How far the label under the thumb is pushed, in px; zero at rest.
    limit: f64,
    /// The size of one label's cell: the spacing between neighbouring labels.
    cell: f64,
    /// How far the active label's bubble is pushed, in px, and its offset from the anchor.
    bubble_push: f64,
    active_dy: f64,
    placed: Vec<Placed>,
}

impl Geometry {
    fn new(viewport: (f64, f64), items: &[&'static str], state: &StripState) -> Self {
        let count = items.len();
        // The list is the caller's and may shrink between renders; a focus left past its
        // end would place the strip off the list.
        let focus = state.focus.min((count - 1) as f64);
        let engaged = state.engaged();
        let active = focus.round().clamp(0.0, (count - 1) as f64) as usize;
        let Metrics { spacing, top, .. } = Metrics::of(viewport, count, engaged);
        let width = viewport.0;

        // How far the thumb has come in from the resting column, and how much
        // window is left for a label to retreat into.
        let room = (width - REST_INSET - spacing / 2.0).max(0.0);
        let inboard = state
            .thumb_x
            .map_or(0.0, |x| (width - x - REST_INSET).max(0.0));
        // The breakpoint under the thumb, then everything out of reach behind it.
        let limit = push_limit(inboard, room);
        let floor = push_floor(limit);
        let reach = reach_for(limit);

        let anchor = anchor_y(focus, spacing, top, if engaged { state.pull } else { 0.0 });
        let placed = place(items, focus, spacing, floor, limit, engaged);
        let active_dy = (active as f64 - focus) * spacing;
        Self {
            active,
            active_label: items[active],
            anchor,
            limit: if engaged { limit } else { 0.0 },
            // Cells tile the spacing exactly, so labels can never overlap however
            // tight the window makes them.
            cell: spacing,
            bubble_push: if engaged {
                push_at(active_dy.abs(), floor, limit, reach)
            } else {
                0.0
            },
            active_dy,
            placed,
        }
    }
}

/// The window-derived layout every transition and the render share. Computed from whether the
/// strip is engaged (it spaces out while a finger is down), so an event always sees the geometry
/// of the state it transitions from rather than whatever the last render captured.
struct Metrics {
    height: f64,
    spacing: f64,
    top: f64,
}

impl Metrics {
    fn of(viewport: (f64, f64), count: usize, engaged: bool) -> Self {
        // A zero-sized window happens before the shell reports a real size; fall back so the
        // rest layout is still sane rather than NaN-ridden.
        let height = if viewport.1 > 0.0 {
            viewport.1
        } else {
            FALLBACK_HEIGHT
        };
        let base = spacing(height, count);
        let spacing = if engaged {
            base * ACTIVE_SPACING_MULTIPLIER
        } else {
            base
        };
        Self {
            height,
            spacing,
            top: top_inset(spacing, count, height),
        }
    }
}

/// Everything the strip remembers, in one value in one signal. Handlers never touch it: they
/// send a [`StripEvent`] and [`StripState::next`] computes the successor, so no two signals can
/// disagree and no handler can hold a borrow across a write.
#[derive(Clone, Debug, Default, PartialEq)]
struct StripState {
    /// Continuous position in breakpoint space. The fractional part drives the in-between
    /// spacing so the strip glides instead of stepping.
    focus: f64,
    /// Where the thumb is horizontally while pressed, `None` at rest: the strip is engaged
    /// exactly while this is set. Its vertical position needs no state: the selection follows
    /// the pointer directly.
    thumb_x: Option<f64>,
    /// How far past either end the pointer is, in px. Absolute, like the selection, so the two
    /// cannot disagree.
    pull: f64,
    /// The last breakpoint reported through `on_select` (and spoken by the live region).
    announced: Option<usize>,
    /// The breakpoint under a hovering pointer, when not pressed.
    hovered: Option<usize>,
    /// Where the owner said the list currently is while a gesture held the strip (a scroll
    /// the gesture itself set off reports back mid-drag): the strip goes there once it rests.
    follow: Option<usize>,
}

impl StripState {
    fn engaged(&self) -> bool {
        self.thumb_x.is_some()
    }
}

/// What can happen to the strip. Positions are in px from the measured box's origin.
#[derive(Clone, Copy, Debug, PartialEq)]
enum StripEvent {
    /// A press on the grab band.
    Press {
        x: f64,
        y: f64,
    },
    /// A pointer move over the overlay; only continues a gesture this strip started.
    Drag {
        x: f64,
        y: f64,
    },
    /// A release or cancel; only ends a gesture this strip started. A release can arrive
    /// without one: a scrollbar drag captures the pointer, so its press never reaches the grab
    /// band, but the release bubbles here when the cursor ends over the band, and engaging
    /// from rest would flash the strip.
    Release,
    /// A pointer move over the grab band, for the hover label.
    Hover {
        y: f64,
    },
    HoverEnd,
    /// A wheel over the index, as travel along the strip in px.
    Wheel {
        travel: f64,
    },
    /// A keyboard step of `by` breakpoints.
    Nudge(f64),
    /// The owner's list is at this breakpoint (it scrolled there): a strip at rest moves its
    /// focus without announcing it, since the owner is not waiting for a selection; a strip
    /// under a thumb keeps it for when the thumb is gone.
    Follow(usize),
}

impl StripState {
    /// The state after `event`, and the breakpoint to report through `on_select` if the event
    /// moved the selection onto a new one. Pure: `viewport` (the measured box's size) and `count` are what the
    /// strip is laid out from.
    fn next(
        &self,
        viewport: (f64, f64),
        count: usize,
        event: StripEvent,
    ) -> (StripState, Option<usize>) {
        // The list is the caller's and may shrink between renders.
        let mut state = Self {
            focus: self.focus.min((count - 1) as f64),
            ..self.clone()
        };
        let mut selected = None;
        let Metrics {
            height,
            spacing,
            top,
        } = Metrics::of(viewport, count, state.engaged());
        match event {
            StripEvent::Press { x, y } => {
                // Start from the breakpoint actually under the finger, and leave the list
                // exactly where it is: only a drag past an end moves it.
                selected = state.commit(focus_at(y, top, spacing, count), count);
                state.thumb_x = Some(x);
                state.pull = pull_for(y, top, spacing, count, height);
                state.hovered = None;
            }
            StripEvent::Drag { x, y } => {
                // No press yet, so there is no gesture to continue.
                if state.engaged() {
                    // Both from the same absolute pointer, so the picked breakpoint stays
                    // exactly under the thumb at every point of the drag.
                    selected = state.commit(focus_at(y, top, spacing, count), count);
                    state.pull = pull_for(y, top, spacing, count, height);
                    state.thumb_x = Some(x);
                }
            }
            StripEvent::Release => {
                if state.engaged() {
                    state.thumb_x = None;
                    state.pull = 0.0;
                    if let Some(index) = state.follow.take() {
                        state.focus = index.min(count - 1) as f64;
                    }
                }
            }
            StripEvent::Hover { y } => {
                if !state.engaged() {
                    state.hovered = Some(
                        focus_at(y, top, spacing, count)
                            .round()
                            .clamp(0.0, (count - 1) as f64) as usize,
                    );
                }
            }
            StripEvent::HoverEnd => state.hovered = None,
            StripEvent::Wheel { travel } => {
                selected = state.commit(state.focus + travel / spacing, count);
            }
            StripEvent::Nudge(by) => {
                selected = state.commit((state.focus + by).round(), count);
            }
            StripEvent::Follow(index) => {
                let index = index.min(count - 1);
                if state.engaged() {
                    state.follow = Some(index);
                } else {
                    state.focus = index as f64;
                }
            }
        }
        (state, selected)
    }

    /// Move the focus to `next` (clamped to the list) and return the breakpoint it lands on if
    /// that is not the one last reported, so each change is reported once.
    fn commit(&mut self, next: f64, count: usize) -> Option<usize> {
        self.focus = next.clamp(0.0, (count - 1) as f64);
        let index = self.focus.round() as usize;
        (self.announced != Some(index)).then(|| {
            self.announced = Some(index);
            index
        })
    }
}

/// The width and height of the measured box.
fn size_of(bounds: PixelsRect) -> (f64, f64) {
    (bounds.size.width, bounds.size.height)
}

/// A pointer's client coordinates as a point in the measured box, which is where every position
/// in the geometry is counted from.
fn local(client: ClientPoint, bounds: PixelsRect) -> Point2D<f64, Pixels> {
    Point2D::new(client.x - bounds.origin.x, client.y - bounds.origin.y)
}

/// The breakpoint step a key asks for, in breakpoints: the sign is the direction, `Home` and
/// `End` ask for more than the list is long so the clamp lands them on the ends.
fn key_step(key: &Key, count: usize) -> Option<f64> {
    match key {
        Key::Named(NamedKey::ArrowDown | NamedKey::ArrowRight | NamedKey::PageDown) => Some(1.0),
        Key::Named(NamedKey::ArrowUp | NamedKey::ArrowLeft | NamedKey::PageUp) => Some(-1.0),
        Key::Named(NamedKey::Home) => Some(-(count as f64)),
        Key::Named(NamedKey::End) => Some(count as f64),
        _ => None,
    }
}

/// The strip covers the window with a fixed overlay and measures that box itself: its size sets
/// the spacing and how far the labels can travel, and its origin is subtracted from the
/// pointer's client coordinates so both live in one space.
#[component]
pub fn ScrubStrip(
    items: Vec<&'static str>,
    on_select: EventHandler<usize>,
    /// The accessible name of the slider.
    #[props(default = "Index".to_string())]
    label: String,
    #[props(default = Renderer::Blitz)] renderer: Renderer,
    /// The item the owner's content is at, when it can say (a list that scrolls reports the item
    /// at its top). The strip's focus follows it while at rest and does not call `on_select`;
    /// while a thumb holds the strip, the last value waits for the release.
    #[props(default)]
    current: Option<usize>,
) -> Element {
    assert!(!items.is_empty(), "ScrubStrip requires at least one item");
    let count = items.len();

    let state = use_signal(StripState::default);
    // The overlay's box in client coordinates, zero-sized until it is measured.
    let mut bounds = use_signal(PixelsRect::default);
    let mut mounted = use_signal(|| None::<Rc<MountedData>>);
    let measure = use_callback(move |()| {
        if !renderer.measures_layout() {
            return;
        }
        let element = mounted
            .peek()
            .clone()
            .expect("a measure follows the overlay's mount");
        spawn(async move {
            let measured = element
                .get_client_rect()
                .await
                .expect("the strip's overlay is mounted and measurable");
            if *bounds.peek() != measured {
                bounds.set(measured);
            }
        });
    });
    // The one place the state is written: copy it out, compute the successor, write it. The
    // `peek` guard ends with the statement that took the copy, so nothing is borrowed while
    // the transition runs or while the caller acts on the selection, which may do anything
    // (the owner's `on_select` may write its own signals).
    let send = use_callback(move |event: StripEvent| {
        let mut state = state;
        let before = state.peek().clone();
        let (after, selected) = before.next(size_of(*bounds.peek()), count, event);
        if after != before {
            state.set(after);
        }
        if let Some(index) = selected {
            on_select.call(index);
        }
    });

    use_effect(use_reactive!(|(current,)| {
        if let Some(index) = current {
            send.call(StripEvent::Follow(index));
        }
    }));

    let view = state();
    let pressed = view.engaged();
    let geometry = Geometry::new(size_of(bounds()), &items, &view);
    let live = view
        .announced
        .and_then(|index| items.get(index).copied())
        .unwrap_or("");
    let ui_state = if pressed {
        "engaged"
    } else if view.hovered.is_some() {
        "hover"
    } else {
        "idle"
    };
    let Geometry {
        active,
        active_label,
        anchor,
        limit,
        cell,
        bubble_push,
        active_dy,
        placed,
    } = geometry;

    rsx! {
        div {
            class: "ct-scrub-strip",
            "data-state": ui_state,
            onmounted: move |event| {
                mounted.set(Some(event.data()));
                measure.call(());
            },
            onresize: move |_| measure.call(()),
            style: "--ct-strip-cell: {cell}px; --ct-strip-inset: {REST_INSET}px; --ct-strip-anchor: {anchor}px; --ct-strip-limit: {limit}px",
            onpointermove: move |event| {
                // Only a pressed strip owns the move; otherwise there is no gesture to
                // continue and the default stays.
                if pressed {
                    event.prevent_default();
                }
                // Fixed-position overlays use viewport coordinates. Page coordinates
                // include document scroll and would move the strip offscreen after scrolling.
                let point = local(event.data().client_coordinates(), *bounds.peek());
                send.call(StripEvent::Drag { x: point.x, y: point.y });
            },
            onpointerup: move |_| send.call(StripEvent::Release),
            onpointercancel: move |_| send.call(StripEvent::Release),
            onwheel: move |event| {
                let delta = event.data().delta().strip_units();
                let travel = if delta.y.abs() > delta.x.abs() { delta.y } else { delta.x };
                if travel != 0.0 {
                    send.call(StripEvent::Wheel { travel });
                }
            },

            div { "data-part": "scrim", "aria-hidden": "true" }

            for (index , item) in placed.iter().enumerate() {
                span {
                    key: "{item.label}",
                    "data-part": "marker",
                    "data-state": if index == active { "active" } else { "inactive" },
                    "data-hover": if view.hovered == Some(index) { Some("") } else { None },
                    "aria-hidden": "true",
                    style: "--ct-strip-push: {item.push}px; --ct-strip-dy: {item.dy}px",
                    span { "data-part": "tick" }
                    span { "data-part": "label", "{item.label}" }
                }
            }

            // Where the thumb is picking, left of the strip. Always mounted: the stylesheet fades
            // it and slides it back to the resting column when the finger lifts, which needs the
            // element to still be there to transition.
            div {
                "data-part": "bubble",
                "aria-hidden": "true",
                style: "--ct-strip-push: {bubble_push}px; --ct-strip-dy: {active_dy}px",
                "{active_label}"
            }

            // Pointer input starts here rather than on the labels: without pointer capture we
            // want one stable target for the whole gesture, and it lets a drag start anywhere
            // along the edge. It is also the slider: one value, the breakpoint.
            div {
                "data-part": "grab-band",
                role: "slider",
                "aria-label": "{label}",
                "aria-orientation": "vertical",
                "aria-valuemin": 1,
                "aria-valuemax": count,
                "aria-valuenow": active + 1,
                "aria-valuetext": "{active_label}",
                tabindex: if renderer.takes_focus() { Some("0") } else { None },
                onpointermove: move |event| {
                    let point = local(event.data().client_coordinates(), *bounds.peek());
                    send.call(StripEvent::Hover { y: point.y });
                },
                onpointerleave: move |_| send.call(StripEvent::HoverEnd),
                onpointerdown: move |event| {
                    event.prevent_default();
                    let point = local(event.data().client_coordinates(), *bounds.peek());
                    send.call(StripEvent::Press { x: point.x, y: point.y });
                },
                onkeydown: move |event| {
                    if let Some(step) = key_step(&event.key(), count) {
                        event.prevent_default();
                        send.call(StripEvent::Nudge(step));
                    }
                },
            }

            // Pressing does not focus the slider, so crossing a breakpoint is spoken here.
            div { "data-part": "status", "aria-live": "polite", "{live}" }
        }
    }
}

#[cfg(test)]
mod tests;
