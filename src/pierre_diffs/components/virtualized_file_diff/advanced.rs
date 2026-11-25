// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/VirtualizedFileDiff.ts
// (C1: the advanced (CodeView) mode; upstream lines 149 (the
// `Virtualizer | CodeView` union), 792-904 (`getNumericScrollAnchor`),
// 909-950 (`getAdvancedStickySpecs`), 1592-1620 (`isSimpleMode`,
// `isAdvancedMode`, `getSimpleVirtualizer`, `getAdvancedVirtualizer`)).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * `virtualizer: Virtualizer | CodeView` is `VirtualizerKind`:
//   `Simple(Rc<Virtualizer>)` or `Advanced(Weak<dyn AdvancedVirtualizer>)`.
//   `CodeView` is not a type here yet (it arrives in C5); what the instance
//   calls on it (`instanceChanged`, `getWindowSpecs`, `getLocalTopForInstance`)
//   is the `AdvancedVirtualizer` trait, which CodeView implements. The `Weak`
//   breaks the item-to-viewer cycle: CodeView owns its items, and an item's
//   instance calls back into the viewer, which outlives it (a dropped viewer is
//   a programming error and panics). `type === 'simple'` is the variant.
//   `isComponentManaged` is not on the trait: the Dioxus view is not
//   component-managed (dropped.psv).
// * The instance is named to the virtualizer by its `InstanceRef` (an `Rc` the
//   viewer compares by allocation, as `Virtualizer` does), not by a concrete
//   `VirtualizedFileDiff`, so the trait is not generic over the instance's
//   type parameters.
// * `getNumericScrollAnchor` is a `DiffLayout` method like `getLinePosition`
//   (see `layout.rs`); the callback's `lineIndex == null` throw is a panic.
//   `getAdvancedStickySpecs` is the instance's (`instance.rs`), it reads the
//   instance's render range.

use std::rc::Rc;
use std::rc::Weak;

use super::layout::DiffLayout;
use super::layout::LayoutContext;
use super::layout::LayoutOptions;
use crate::components::virtualizer::InstanceRef;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerHost;
use crate::types::layout::VirtualWindowSpecs;
use crate::types::options::DiffStyle;
use crate::types::view::AnnotationSide;
use crate::types::view::NumericScrollLineAnchor;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_header_region;
use crate::utils::iterate_over_diff::DiffLineCallbackProps;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;
use crate::utils::virtual_diff_layout::HunkSeparatorBaseProps;
use crate::utils::virtual_diff_layout::get_leading_hunk_separator_layout;
use crate::utils::virtual_diff_layout::get_trailing_hunk_separator_layout;

/// What an instance asks of `CodeView` (`type === 'advanced'`): the members of
/// the class that `VirtualizedFileDiff` reads through its `virtualizer` field.
pub trait AdvancedVirtualizer {
    /// `instanceChanged(instance, layoutDirty)`
    fn instance_changed(&self, instance: &InstanceRef, layout_dirty: bool);
    /// `getWindowSpecs()`
    fn get_window_specs(&self) -> VirtualWindowSpecs;
    /// `getLocalTopForInstance(instance)`: panics for an unregistered instance.
    fn get_local_top_for_instance(&self, instance: &InstanceRef) -> f64;
}

/// `Virtualizer | CodeView`.
pub enum VirtualizerKind<Host: VirtualizerHost> {
    Simple(Rc<Virtualizer<Host>>),
    Advanced(Weak<dyn AdvancedVirtualizer>),
}

impl<Host: VirtualizerHost> VirtualizerKind<Host> {
    /// `isSimpleMode()`
    pub fn is_simple(&self) -> bool {
        matches!(self, Self::Simple(_))
    }

    /// `isAdvancedMode()`
    pub fn is_advanced(&self) -> bool {
        matches!(self, Self::Advanced(_))
    }

    /// `getSimpleVirtualizer()`
    pub fn simple(&self) -> Option<&Rc<Virtualizer<Host>>> {
        match self {
            Self::Simple(virtualizer) => Some(virtualizer),
            Self::Advanced(_) => None,
        }
    }

    /// `getAdvancedVirtualizer()`
    pub fn advanced(&self) -> Option<Rc<dyn AdvancedVirtualizer>> {
        match self {
            Self::Simple(_) => None,
            Self::Advanced(code_view) => Some(upgrade(code_view)),
        }
    }

    /// `this.virtualizer.getWindowSpecs()`
    pub fn get_window_specs(&self) -> VirtualWindowSpecs {
        match self {
            Self::Simple(virtualizer) => virtualizer.get_window_specs(),
            Self::Advanced(code_view) => upgrade(code_view).get_window_specs(),
        }
    }

    /// `this.virtualizer.instanceChanged(instance, layoutDirty)`
    pub fn instance_changed(&self, instance: &InstanceRef, layout_dirty: bool) {
        match self {
            Self::Simple(virtualizer) => virtualizer.instance_changed(instance, layout_dirty),
            Self::Advanced(code_view) => {
                upgrade(code_view).instance_changed(instance, layout_dirty)
            }
        }
    }
}

fn upgrade(code_view: &Weak<dyn AdvancedVirtualizer>) -> Rc<dyn AdvancedVirtualizer> {
    code_view
        .upgrade()
        .expect("the CodeView outlives the instances it owns")
}

impl DiffLayout {
    /// `getNumericScrollAnchor`: the first row at or below `local_viewport_top`.
    pub fn get_numeric_scroll_anchor(
        &mut self,
        local_viewport_top: f64,
        ctx: &LayoutContext<'_>,
    ) -> Option<NumericScrollLineAnchor> {
        let file_diff = ctx.layout_diff?;
        let LayoutOptions {
            disable_file_header,
            collapsed,
            collapsed_context_threshold,
            diff_style,
            hunk_separators,
            ..
        } = ctx.options;
        if collapsed {
            return None;
        }

        self.approximate_layout_checkpoints(file_diff, ctx);
        let checkpoint = self.get_layout_checkpoint_before_top(local_viewport_top, None);
        let mut top = checkpoint.map_or(
            get_virtual_file_header_region(&self.metrics, disable_file_header)
                + self.cache.file_annotation_height,
            |checkpoint| checkpoint.top,
        );
        let mut anchor: Option<NumericScrollLineAnchor> = None;
        let separator_props = HunkSeparatorBaseProps {
            separator_type: hunk_separators,
            metrics: &self.metrics,
        };

        // This may end up being quite expensive on extremely large files, we may
        // need to figure out how to anchor on different regions, or utilize
        // renderRange to shortcut this for us somehow
        iterate_over_diff(
            IterateOverDiffProps {
                starting_line: checkpoint.map_or(0, |checkpoint| checkpoint.rendered_line_index),
                expanded_hunks: Some(ctx.effective_expanded_hunks()),
                collapsed_context_threshold,
                ..IterateOverDiffProps::new(
                    file_diff,
                    match diff_style {
                        DiffStyle::Split => IterationDiffStyle::Split,
                        DiffStyle::Unified => IterationDiffStyle::Unified,
                    },
                )
            },
            |DiffLineCallbackProps {
                 hunk_index,
                 hunk,
                 collapsed_before,
                 collapsed_after,
                 deletion_line,
                 addition_line,
                 ..
             }| {
                let line_index = match diff_style {
                    DiffStyle::Split => addition_line
                        .map(|line| line.split_line_index)
                        .or(deletion_line.map(|line| line.split_line_index)),
                    DiffStyle::Unified => addition_line
                        .map(|line| line.unified_line_index)
                        .or(deletion_line.map(|line| line.unified_line_index)),
                }
                .expect("VirtualizedFileDiff.getNumericScrollAnchor: missing line index data")
                    as i64;

                if collapsed_before > 0 {
                    let separator = get_leading_hunk_separator_layout(
                        separator_props,
                        hunk_index,
                        hunk.and_then(|hunk| hunk.hunk_specs.as_deref()),
                    );
                    if let Some(separator) = separator {
                        top += separator.total_height;
                    }
                }

                if top >= local_viewport_top {
                    if let Some(deletion_line) = deletion_line {
                        anchor = Some(NumericScrollLineAnchor {
                            line_number: deletion_line.line_number,
                            side: Some(AnnotationSide::Deletions),
                            top,
                        });
                    } else if let Some(addition_line) = addition_line {
                        anchor = Some(NumericScrollLineAnchor {
                            line_number: addition_line.line_number,
                            side: Some(AnnotationSide::Additions),
                            top,
                        });
                    }
                    if anchor.is_some() {
                        return true;
                    }
                }

                top += self.get_line_height(
                    line_index,
                    addition_line.is_some_and(|line| line.no_eofcr)
                        || deletion_line.is_some_and(|line| line.no_eofcr),
                );

                if collapsed_after > 0 {
                    let separator = get_trailing_hunk_separator_layout(separator_props);
                    if let Some(separator) = separator {
                        top += separator.total_height;
                    }
                }

                false
            },
        );

        anchor
    }
}
