//! Port of `packages/diffs/src/components/VirtualizedFileDiff.ts`, in slices.
//! S12b-1 is the layout model (`layout`): pure over `FileDiffMetadata`, the
//! options and the layout cache, no DOM. The window and measurement (S12b-2)
//! and the integration with `FileDiff` (S12b-3) are not ported yet.

pub mod layout;

#[cfg(test)]
mod layout_oracle_tests;
#[cfg(test)]
mod layout_test_instance;
#[cfg(test)]
mod layout_upstream_tests;
