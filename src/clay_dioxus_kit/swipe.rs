//! The drawer's swipe-and-snap engine: a line-by-line port of the pure logic of Base UI's
//! `Drawer` (velocity estimation, release decisions, snap-point resolution, momentum projection,
//! rubber-banding, in all four directions), with no DOM and no Dioxus types.
//!
//! Upstream: <https://github.com/mui/base-ui> at tag `v1.9.0`
//! (`ad7ecda5296b0295b41a51ecb01fce6204acd099`), `packages/react/src/`. Every function carries the
//! upstream file and line it ports, under the upstream name in `snake_case`; each submodule is
//! one upstream file. The constants keep upstream's values. Behaviour that upstream spreads over
//! the DOM (reading `offsetWidth`, `getComputedStyle(..).transform`, writing CSS variables,
//! `setPointerCapture`) is taken as an input or returned as an output instead: a caller measures
//! and applies, this module decides. What is deliberately not ported is listed in the S1 notes of
//! `docs/scratch/dayframe-shell-ledger.md`.
//!
//! Base UI is licensed under the MIT License:
//!
//! ```text
//! MIT License
//!
//! Copyright (c) 2019 Material-UI SAS
//!
//! Permission is hereby granted, free of charge, to any person obtaining a copy
//! of this software and associated documentation files (the "Software"), to deal
//! in the Software without restriction, including without limitation the rights
//! to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//! copies of the Software, and to permit persons to whom the Software is
//! furnished to do so, subject to the following conditions:
//!
//! The above copyright notice and this permission notice shall be included in all
//! copies or substantial portions of the Software.
//!
//! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//! IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//! FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//! AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//! LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//! OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//! SOFTWARE.
//! ```
//!
//! Nesting: Base UI nests drawers by CSS (`--nested-drawers`, `--drawer-frontmost-height`,
//! `--drawer-swipe-progress`) driven by the progress numbers computed here
//! ([`drawer_viewport::apply_swipe_progress`], [`drawer_viewport::update_nested_swipe_active`]),
//! so a stack of drawers needs one [`use_swipe_dismiss::SwipeDismiss`] per drawer and nothing
//! else in this module.

pub mod drawer_popup;
pub mod drawer_swipe_area;
pub mod drawer_viewport;
pub mod use_drawer_snap_points;
pub mod use_swipe_dismiss;

/// A point or a 2D delta, in logical px (the `{ x, y }` objects upstream passes around).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

/// An element's `offsetWidth` x `offsetHeight`, measured by the caller.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Size {
    pub width: f64,
    pub height: f64,
}

/// `packages/utils/src/clamp.ts:1-6`: `Math.max(min, Math.min(val, max))`. Not `f64::clamp`,
/// which panics when `min > max`; this one lets `min` win, as upstream does.
pub(crate) fn clamp(value: f64, min: f64, max: f64) -> f64 {
    min.max(value.min(max))
}

/// JavaScript's `Math.sign`: `0` and `-0` stay themselves, where `f64::signum` says `1`.
pub(crate) fn sign(value: f64) -> f64 {
    if value > 0.0 {
        1.0
    } else if value < 0.0 {
        -1.0
    } else {
        value
    }
}
