//! Throwaway probe for the infinite-canvas design. Delete once the camera is
//! decided; the findings belong in the canvas FDR, not this file.
//!
//! It exists to answer three questions before any canvas architecture is
//! committed to, because a camera that cannot pan cheaply is not a camera:
//!
//! 1. **Does a `transform` on a wrapper node actually move a real tile tree in
//!    the native renderer?** Reading says yes — `blitz-dom` resolves
//!    `transform`/`translate`/`scale`/`rotate` with `transform-origin`
//!    (`stylo_to_kurbo.rs`) and `blitz-paint` applies it as a stack
//!    (`render.rs`: `parent_style_transform * node.transform()`).
//!
//! 2. **Does the input path follow the transform?** Reading says yes, and this
//!    is the part that decides whether a transform camera is honest:
//!    `EventDriver::handle_pointer_move` → `Document::set_hover_to` →
//!    `Document::hit` → `Node::hit`, which converts the point into the node's
//!    space with `t.inverse()` before testing the box. So the target node, and
//!    therefore every pointer event, is resolved through the transform.
//!
//! 3. **What does a transform change cost?** This is the open question. A
//!    transform is a paint-time effect, so a pan should be O(1) in the tree —
//!    but `resolve_transforms` walks the tree, and a full walk per frame would
//!    make panning O(subtree) and rule the transform camera out. Nothing static
//!    answers this; it has to be measured.
//!
//! Question 3 needs a transform that keeps changing, and the machine cannot
//! fake pointer input into a Wayland client, so the probe pans *itself*: a
//! triangle wave on `pan`. Build with the `log-times` feature on `dioxus-native`
//! and Blitz prints its own per-phase timings for every resolve pass, so the
//! cost shows up on stdout without the app having to measure anything.
//!
//! `DAYFRAME_PROBE_COLUMNS` sets how many columns the world holds, so the same
//! probe can be run at a few sizes: if the per-phase timings stay flat as the
//! count grows, a transform change is O(1) in the tree and the camera is free.
//! `DAYFRAME_PROBE_AUTOPAN=1` makes the camera sweep itself, for measuring a
//! moving camera without pointer input. Off by default: it is a measuring
//! instrument, not a camera anyone would want to use.
//!
//! # Measured
//!
//! 1076x860 window, dev build with the `desktop-dev` opt profile, real tile
//! columns, self-panning at 16ms. `transform` is Blitz's own timer, so these
//! exclude the cost of printing them:
//!
//! | world | `style` | `flush` | `layout` | `transform` | pass |
//! |---|---|---|---|---|---|
//! | 12 columns, panning | 1.1ms | 202µs | 104µs | **5µs** | ~2.0ms |
//! | 40 columns, panning | 4.5ms | 1.2ms | 305µs | **8µs** | ~6.9ms |
//! | 12 columns, still | 11µs | 435µs | 117µs | **160ns** | 6 passes in 22s |
//!
//! What that says:
//!
//! - **The camera itself is free.** `transform` costs 5-8µs and is near-flat in
//!   tree size, because `resolve_transforms` is a cheap walk and the transform
//!   is applied when painting, not when laying out. Nothing re-lays-out *because
//!   of the transform*.
//! - **The cost is the style invalidation, not the property.** The camera node is
//!   an ancestor of the whole world, so dirtying its inline style restyles that
//!   whole world: `style` + `flush` + `layout` scale with the DOM, 2.0ms at 12
//!   columns to 6.9ms at 40. Panning by `left`/`top` would dirty exactly the same
//!   traversal, so the mechanism does not matter — the *frequency* does. At 40
//!   columns this still fits a 60fps budget; it is the trend that does not.
//! - **Idle is genuinely free.** Held still, Blitz skips the pass entirely: six
//!   resolves in 22 seconds, `transform` 160ns. A settled camera costs nothing,
//!   so an animated one only costs while it is moving.
//! - **Culling is what makes it scale, and it has to be hysteretic.** The DOM is
//!   the cost, so the retained set must be bounded — but inserting and removing
//!   nodes mid-pan costs more than the style change it would avoid, so culling
//!   needs a margin to stay quiet while the camera moves.
//! - **A scroll offset would sidestep the invalidation entirely.** Blitz's scroll
//!   offset is not a style change: it is a stored offset that paint, hit testing
//!   (`x - location + scroll_offset`) and the wheel path already honour, so
//!   panning a scroll container should be O(1) in the DOM. `blitz-dom` has
//!   `Document::scroll_to` and `scroll_node_by`; dioxus-native's `scroll_to` and
//!   `scroll` return `NotSupported`, so this needs a small patch. Worth doing if
//!   panning a large world turns out to be too slow.
//! - **AccessKit carries no geometry in this version** — there is not a single
//!   `Bounds` in `blitz-dom` or `blitz-shell` — so the usual objection to a
//!   transform camera (a11y rectangles left in world space) does not apply yet.
//!   If bounds ever land upstream, they will need transforming for this camera.

use dioxus::prelude::*;
use std::time::Duration;

use crate::tiles::{sample_columns, Column, TileList};

/// Width of one column in world px.
const COLUMN_WIDTH: f64 = 320.0;
/// Gap between columns in world px.
const COLUMN_GAP: f64 = 28.0;
/// How far the self-pan swings either side of the origin, in px.
const AUTOPAN_LIMIT: f64 = 160.0;
/// Self-pan speed in px per tick. Big enough to be one transform change per
/// frame, slow enough to read.
const AUTOPAN_STEP: f64 = 2.0;
/// The frame cadence the self-pan aims for.
const AUTOPAN_TICK: Duration = Duration::from_millis(16);
/// How much one wheel notch changes the zoom.
const ZOOM_PER_NOTCH: f64 = 0.002;
/// Zoom range. Below this the world is a thumbnail; above it, a magnifier.
const ZOOM_RANGE: (f64, f64) = (0.15, 2.5);

/// How many columns to build. A knob rather than a constant because the point of
/// the probe is the *shape* of the cost curve, which one size cannot show.
fn world_columns() -> usize {
    std::env::var("DAYFRAME_PROBE_COLUMNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(12)
}

/// Off by default. The sweep exists only so the *cost* of a moving camera can be
/// measured without pointer input; it makes the camera feel like a screensaver,
/// which is the opposite of what the probe is for when someone is feeling it out.
fn autopan() -> bool {
    std::env::var("DAYFRAME_PROBE_AUTOPAN")
        .map(|v| v != "0")
        .unwrap_or(false)
}

/// A pan-and-zoom camera over a world of tile columns.
///
/// The whole camera is two numbers on one node's `transform`. Pan is applied
/// *outside* the scale (`translate() scale()` composes as `T·S`, so the scale
/// happens first and the pan is in screen px), which keeps the drag
/// one-for-one: a hundred pixels of movement moves the world a hundred pixels
/// at any zoom.
#[component]
pub fn CanvasProbe(viewport: (f64, f64)) -> Element {
    let count = world_columns();
    let columns = use_signal(move || {
        // Cycle the sample so the world is long enough to pan across without
        // hand-writing dozens of columns.
        let sample = sample_columns();
        (0..count)
            .map(|i| sample[i % sample.len()].clone())
            .collect::<Vec<Column>>()
    });

    let mut pan = use_signal(|| (0.0f64, 0.0f64));
    let mut zoom = use_signal(|| 1.0f64);
    // Press point in window px, plus the pan at the moment of the press. The
    // camera resolves from absolute positions, never from accumulated deltas,
    // for the same reason the arc index does: one input, one frame.
    let mut grab = use_signal(|| None::<(f64, f64, f64, f64)>);

    if autopan() {
        let mut direction = use_signal(|| AUTOPAN_STEP);
        use_future(move || async move {
            let mut ticker = tokio::time::interval(AUTOPAN_TICK);
            loop {
                ticker.tick().await;
                let (x, y) = *pan.peek();
                let mut step = *direction.peek();
                if !(-AUTOPAN_LIMIT..=AUTOPAN_LIMIT).contains(&(x + step)) {
                    step = -step;
                    direction.set(step);
                }
                pan.set((x + step, y));
            }
        });
    }

    let (width, height) = viewport;
    let (pan_x, pan_y) = pan();
    let zoom_now = zoom();

    // `translate` before `scale` so the pan is screen-space: see the doc comment.
    let world_transform = format!("translate({pan_x}px, {pan_y}px) scale({zoom_now})");

    rsx! {
        div {
            style: "position:absolute; inset:0; overflow:hidden; background:#0d0e10;",

            // The camera itself: one node, two numbers. Everything below is in
            // world coordinates and never moves in its own right.
            div {
                style: "position:absolute; left:0; top:0; transform:{world_transform}; transform-origin:0 0; display:flex; align-items:flex-start; gap:{COLUMN_GAP}px; padding:40px;",
                for (i, column) in columns().into_iter().enumerate() {
                    div {
                        key: "{i}",
                        style: "width:{COLUMN_WIDTH}px; flex:0 0 auto;",
                        div {
                            style: "color:#9ca3af; font-family:system-ui,sans-serif; font-size:12px; letter-spacing:0.08em; text-transform:uppercase; padding:0 0 8px 2px;",
                            "{column.name} · {i}"
                        }
                        // The probe is about the camera, not sublists, so
                        // opening is inert here.
                        TileList {
                            tiles: column.tiles,
                            depth: 0,
                            spawner: None,
                            on_open: move |_| {},
                        }
                    }
                }
            }

            // Input surface over the whole window, so a drag that starts inside
            // the world keeps receiving moves once it leaves the world's box.
            // Transparent, and above the world for hit testing only.
            div {
                style: "position:absolute; inset:0;",
                onwheel: move |event| {
                    let data = event.data();
                    let delta = data.delta().strip_units();
                    let point = data.page_coordinates();
                    let (px, py) = *pan.peek();

                    if data.modifiers().ctrl() || data.modifiers().meta() {
                        // Zoom about the pointer: the world point under the
                        // cursor stays under the cursor. `screen = world·z − pan`,
                        // so holding `(s + pan)/z` fixed gives
                        // `pan' = pan·k + s·(k − 1)` for `k = z'/z`.
                        let old = *zoom.peek();
                        let next = (old * (1.0 - delta.y * ZOOM_PER_NOTCH))
                            .clamp(ZOOM_RANGE.0, ZOOM_RANGE.1);
                        let k = next / old;
                        pan.set((px * k + point.x * (k - 1.0), py * k + point.y * (k - 1.0)));
                        zoom.set(next);
                    } else {
                        pan.set((px - delta.x, py - delta.y));
                    }
                    event.stop_propagation();
                },
                onpointerdown: move |event| {
                    let point = event.data().page_coordinates();
                    let (px, py) = *pan.peek();
                    grab.set(Some((point.x, point.y, px, py)));
                },
                onpointermove: move |event| {
                    let Some((start_x, start_y, pan_x, pan_y)) = *grab.peek() else {
                        return;
                    };
                    let point = event.data().page_coordinates();
                    pan.set((pan_x + (point.x - start_x), pan_y + (point.y - start_y)));
                },
                onpointerup: move |_| grab.set(None),
                onpointercancel: move |_| grab.set(None),
            }

            // Readout: the camera, and the window size the camera thinks it is
            // zooming about. Kept on screen because a probe that cannot report
            // its own state is a screenshot.
            div {
                style: "position:absolute; left:12px; bottom:12px; font-family:ui-monospace,monospace; font-size:11px; color:#6b7280; background:rgba(13,14,16,0.85); padding:6px 9px; border-radius:6px; pointer-events:none;",
                "{count} columns · {width:.0}x{height:.0} · pan {pan_x:.0},{pan_y:.0} · zoom {zoom_now:.2}"
            }
        }
    }
}
