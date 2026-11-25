//! Port of `packages/diffs/src/components/VirtualizedFileDiff.ts`, in slices.
//! S12b-1 is the layout model (`layout`): pure over `FileDiffMetadata`, the
//! options and the layout cache, no DOM. S12b-2 is the window (`window`:
//! `getLinePosition`, `computeRenderRangeFromWindow`) and the instance's
//! measurement and bookkeeping (`instance`: `reconcileHeights`, `onRender`,
//! `setVisibility`, `updatePendingRender`, `updateCodeViewLayout`). The
//! integration with `FileDiff` (S12b-3) is `overrides`, and C1 is the advanced
//! (CodeView) mode (`advanced`: the `Virtualizer | CodeView` union).

pub mod advanced;
pub mod instance;
pub mod layout;
pub mod overrides;
pub mod window;

#[cfg(test)]
mod advanced_tests;
#[cfg(test)]
mod advanced_upstream_tests;
#[cfg(test)]
mod integration_upstream_tests;
#[cfg(test)]
pub(crate) mod layout_oracle_tests;
#[cfg(test)]
mod layout_test_instance;
#[cfg(test)]
pub(crate) mod layout_upstream_tests;
#[cfg(test)]
mod padding_upstream_tests;
#[cfg(test)]
mod window_churn_test;
#[cfg(test)]
mod window_oracle_tests;
#[cfg(test)]
mod window_upstream_tests;
