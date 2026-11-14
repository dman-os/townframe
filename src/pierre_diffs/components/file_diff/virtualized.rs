// Part of the S12b-2 port of VirtualizedFileDiff.ts (see
// ../virtualized_file_diff/instance.rs): what `VirtualizedFileDiff`, which
// extends `FileDiff`, reads and writes of the fields it inherits. Upstream
// reads them as `this.renderRange`, `this.placeHolder`, `this.codeAdditions`...;
// here the state is private to `FileDiff`, so a view hands out exactly those
// fields. A view over the instance (see `file_diff_view!`). Original to this
// crate: no counterpart upstream, except `render_placeholder` with
// `set_placeholder_height` and `clean_child_nodes` (FileDiff.ts 2675-2760,
// public API that only the subclass calls). `cleanChildNodes` follows
// `Nodes::remove_rendered_code` and `cleanUp` for ownership: the managers are
// cleaned up first, so the nodes it drops are discarded at once.

use super::lifecycle::parse_input;
use super::*;
use crate::types::layout::RenderRange;
use crate::types::lines::ExpansionDirections;
use crate::types::patch::DiffFileInput;
use crate::utils::predicates::are_files_equal::are_files_equal;

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

    /// `const { forceRenderOverride } = this; this.forceRenderOverride = undefined`
    pub fn take_force_render_override(&self) -> bool {
        std::mem::take(&mut self.state.borrow_mut().force_render_override)
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

    /// `getOrCreateFileContainer(fileContainer)`: no wrapper.
    pub fn get_or_create_file_container(&self, file_container: Option<NodeKey>) -> NodeKey {
        Nodes(self.0).get_or_create_file_container(file_container, None)
    }

    /// `parseDiffFromFile(oldFile, newFile, this.options.parseDiffOptions)`
    pub fn parse_input(&self, input: &DiffFileInput) -> Rc<FileDiffMetadata> {
        parse_input(input, &Lifecycle(self.0).parse_diff_options())
    }

    /// `!areOptionalFilesEqual(oldFile, this.deletionFile) ||
    /// !areOptionalFilesEqual(newFile, this.additionFile)`
    pub fn files_differ(
        &self,
        old_file: Option<&FileContents>,
        new_file: Option<&FileContents>,
    ) -> bool {
        let state = self.state.borrow();
        !are_files_equal(old_file, state.deletion_file.as_ref())
            || !are_files_equal(new_file, state.addition_file.as_ref())
    }

    /// `this.deletionFile = oldFile; this.additionFile = newFile`
    pub fn set_files(&self, old_file: Option<&FileContents>, new_file: Option<&FileContents>) {
        let mut state = self.state.borrow_mut();
        state.deletion_file = old_file.cloned();
        state.addition_file = new_file.cloned();
    }

    /// `this.enabled && (this.getInputDiff() != null || this.additionFile != null
    /// || this.deletionFile != null)`: what `rerender` checks first.
    pub fn can_rerender(&self) -> bool {
        let state = self.state.borrow();
        state.enabled
            && (state.file_diff.is_some()
                || state.addition_file.is_some()
                || state.deletion_file.is_some())
    }

    /// `requestRender()`: an owner-driven instance asks its owner to render.
    pub fn request_render(&self) -> bool {
        let Some(request_owner_render) = &self.request_owner_render else {
            return false;
        };
        self.state.borrow_mut().force_render_override = true;
        request_owner_render();
        true
    }

    /// `this.lineStateRefreshPending`
    pub fn line_state_refresh_pending(&self) -> bool {
        self.state.borrow().line_state_refresh_pending
    }

    /// `renderPlaceholder(height)`: replace everything rendered by one
    /// placeholder of the layout's height.
    pub fn render_placeholder(&self, height: f64) -> bool {
        let Some(file_container) = self.state.borrow().file_container else {
            return false;
        };
        Lifecycle(self.0).emit_post_render(true);
        self.clean_child_nodes();

        if self.state.borrow().place_holder.is_none() {
            let document = self.document();
            let shadow_root = Nodes(self.0).shadow_root_of(file_container);
            let place_holder = document.create_element("div");
            document.set_attribute(place_holder, "data-placeholder", "");
            document.append_child(shadow_root, place_holder);
            self.state.borrow_mut().place_holder = Some(place_holder);
        }
        self.set_placeholder_height(height)
    }

    fn set_placeholder_height(&self, height: f64) -> bool {
        let Some(place_holder) = self.state.borrow().place_holder else {
            return false;
        };
        self.document()
            .set_style_property(place_holder, "height", &format!("{height}px"));
        true
    }

    fn clean_child_nodes(&self) {
        let nodes = Nodes(self.0);
        // The managers' clean ups, the buffers, the code groups and `pre`.
        nodes.remove_rendered_code();
        nodes.clear_auxiliary_nodes();
        let (header_element, sprite_svg, theme_css_style, unsafe_css_style) = {
            let mut state = self.state.borrow_mut();
            (
                state.header_element.take(),
                state.sprite_svg.take(),
                state.theme_css_style.take(),
                state.unsafe_css_style.take(),
            )
        };
        nodes.drop_optional(header_element);
        nodes.clear_header_slots();
        nodes.drop_optional(sprite_svg);
        nodes.drop_optional(theme_css_style);
        nodes.drop_optional(unsafe_css_style);
        {
            let mut state = self.state.borrow_mut();
            state.applied_theme_css = None;
            state.has_adopted_theme_css = false;
            state.applied_unsafe_css = None;
            state.header_cache.last_rendered_header = None;
            state.last_row_count = None;
            state.mounted = false;
        }
        // The managers cleaned up above hold no key of what this dropped.
        nodes.discard_retired();
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
