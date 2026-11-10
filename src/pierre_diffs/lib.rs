//! Pure-logic port of `@pierre/diffs` 1.6.4 (Pierre Computer Company,
//! Apache-2.0); diffing goes through the `similar` crate behind [`diff`], which
//! keeps the shape of the four jsdiff 9.0.0 calls Pierre makes.
//!
//! The crate has no engine, UI or I/O dependencies so it builds for native,
//! `wasm32-unknown-unknown` and `wasm32-wasip3`. Ported files each open with a
//! header naming the upstream file and tag they derive from and listing the
//! deviations (Apache-2.0 section 4); `PARITY.md` maps every upstream file to
//! its Rust location or to an explicit OUT/DEFER reason, and
//! `oracle/` holds the differential oracle that runs the published JavaScript
//! and writes the goldens the tests compare against.
//!
//! Offsets follow the policy documented in [`utf16`].

pub mod components;
pub mod constants;
pub mod diff;
pub mod hast;
pub mod highlighter_types;
pub mod highlights;
pub mod js_collections;
pub mod js_text;
pub mod managers;
pub mod renderers;
pub mod scanners;
pub mod sprite;
pub mod types;
pub mod utf16;
pub mod utils;

#[cfg(test)]
mod fake_dom;
#[cfg(test)]
mod oracle_testing;
#[cfg(test)]
mod parity;
#[cfg(test)]
mod test_utils;
