// Part of the S12b-2 port of VirtualizedFileDiff.ts (see
// ../virtualized_file_diff/instance.rs): what `VirtualizedFileDiff`, which
// extends `FileDiff`, reads and writes of the fields it inherits. Upstream
// reads them as `this.renderRange`, `this.placeHolder`, `this.codeAdditions`...;
// here the state is private to `FileDiff`, so a view hands out exactly those
// fields. A view over the instance (see `file_diff_view!`). Original to this
// crate: no counterpart upstream.

use super::*;
use crate::types::layout::RenderRange;
use crate::types::lines::ExpansionDirections;

file_diff_view!(pub Virtualized);

/// `codeDeletions`, `codeAdditions` and `codeUnified`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CodeGroups {
    pub deletions: Option<NodeKey>,
    pub additions: Option<NodeKey>,
    pub unified: Option<NodeKey>,
}

impl<Host, Rendered, LAnnotation> Virtualized<'_, Host, Rendered, LAnnotation>
where
    Host: FileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    pub fn render_range(&self) -> Option<RenderRange> {
        self.state.borrow().render_range
    }

    /// `this.renderRange = undefined` and the like.
    pub fn set_render_range(&self, render_range: Option<RenderRange>) {
        self.state.borrow_mut().render_range = render_range;
    }

    pub fn place_holder(&self) -> Option<NodeKey> {
        self.state.borrow().place_holder
    }

    pub fn file_container(&self) -> Option<NodeKey> {
        self.state.borrow().file_container
    }

    pub fn code_groups(&self) -> CodeGroups {
        let state = self.state.borrow();
        CodeGroups {
            deletions: state.code_deletions,
            additions: state.code_additions,
            unified: state.code_unified,
        }
    }

    /// `this.forceRenderOverride = true`
    pub fn force_render_override(&self) {
        self.state.borrow_mut().force_render_override = true;
    }

    /// `this.hunksRenderer.expandHunk(...)`: the simple-mode `expandHunk`'s
    /// renderer half (the layout resets and the notifications are the caller's).
    pub fn expand_renderer_hunk(
        &self,
        hunk_index: usize,
        direction: ExpansionDirections,
        expansion_line_count_override: Option<usize>,
    ) {
        self.hunks_renderer.borrow_mut().expand_hunk(
            hunk_index,
            direction,
            expansion_line_count_override,
        );
    }

    /// `updateInputDiff`: store a new input; `false` if it has not changed.
    pub fn update_input_diff(
        &self,
        incoming_diff: Rc<FileDiffMetadata>,
        line_annotations: Option<LineAnnotations<LAnnotation>>,
    ) -> bool {
        Lifecycle(self.0).update_input_diff(incoming_diff, line_annotations)
    }

    /// Test-only: what the upstream tests write into the private fields to
    /// stand in for a render (`renderedData`, `fileContainer`, `codeAdditions`...).
    #[cfg(test)]
    pub fn install_rendered_for_test(
        &self,
        file_diff: &Rc<FileDiffMetadata>,
        file_container: Option<NodeKey>,
        code_groups: CodeGroups,
        place_holder: Option<NodeKey>,
    ) {
        let line_annotations = self.get_latest_annotations();
        let mut state = self.state.borrow_mut();
        state.rendered_data = Some(FileDiffRenderData {
            file_diff: Rc::clone(file_diff),
            line_annotations,
        });
        state.file_container = file_container;
        state.code_deletions = code_groups.deletions;
        state.code_additions = code_groups.additions;
        state.code_unified = code_groups.unified;
        state.place_holder = place_holder;
    }

    /// Test-only: `this.forceRenderOverride === true`.
    #[cfg(test)]
    pub fn is_force_render_override(&self) -> bool {
        self.state.borrow().force_render_override
    }
}
