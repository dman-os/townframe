// Part of the S12a-2b port of FileDiff.ts (see ../file_diff.rs for the header):
// the render orchestration and hydration: `render`, `hydrate`,
// `hydrateElements`, `hydrationSetup`, `updateInputDiff`, `adoptRenderData`,
// `prepareRenderData`, `emitPostRender`. A view over the instance (see
// `file_diff_view!`).
// Changes (besides the ones in the main header):
// * Errors are programming errors and panic: the `try`/`catch` around the
//   collapsed and the full render (log the error, or `applyErrorToDOM` when
//   `disableErrorHandling` is false) is dropped with `applyErrorToDOM`,
//   `errorWrapper` and `cleanupErrorWrapper` (nothing else sets the wrapper).
//   `disable_error_handling` is therefore not read by `render`.
// * `hunksRenderer.getDiffForNextRender(workingDiff)` is the working diff and
//   `hunksRenderer.hydrate(fileDiff)` does nothing: the Rust renderer is
//   synchronous and has no render cache (see dropped.psv). `renderDiff` never
//   returns `undefined`, so the worker pool initialisation branch is gone.
//   `HunksRenderResult.fileDiff` is the diff handed to `render_diff`.
// * `finalizeRender` is an empty hook upstream that `VirtualizedFileDiff`
//   overrides; it is not ported here (S12b decides how the subclass hooks are
//   expressed).
// * The editor, edit session (`editSessionDirty`, `finishEditSessionForDiff`,
//   `syncRenderViewToEditor`, the `forceEditorRender` of `hydrate`), the worker
//   pool and `prerenderedHTML` (`prerenderHTMLIfNecessary`) are dropped.
// * `onPostRender` is called without the instance.
// * `requestRender` is inlined in `rerender`; `rerender` is on `FileDiff` itself
//   (it is public API).

use std::rc::Rc;

use super::*;
use crate::constants::DEFAULT_RENDER_RANGE;
use crate::constants::EMPTY_RENDER_RANGE;
use crate::constants::THEME_CSS_ATTRIBUTE;
use crate::constants::UNSAFE_CSS_ATTRIBUTE;
use crate::types::patch::DiffFileInput;
use crate::utils::options::get_diff_file_input::get_diff_file_input;
use crate::utils::parse_diff_from_file::parse_diff_from_file;
use crate::utils::predicates::are_files_equal::are_files_equal;
use crate::utils::predicates::are_render_ranges_equal::are_render_ranges_equal;

file_diff_view!(pub Lifecycle);

fn parse_input(
    input: &DiffFileInput,
    options: &crate::types::patch::ParseDiffOptions,
) -> Rc<FileDiffMetadata> {
    Rc::new(
        parse_diff_from_file(input, options, false)
            .expect("FileDiff: the files could not be diffed")
            .value,
    )
}

fn input_files(input: Option<&DiffFileInput>) -> (Option<&FileContents>, Option<&FileContents>) {
    match input {
        None => (None, None),
        Some(DiffFileInput::Both { old_file, new_file }) => (Some(old_file), Some(new_file)),
        Some(DiffFileInput::Added { new_file }) => (None, Some(new_file)),
        Some(DiffFileInput::Deleted { old_file }) => (Some(old_file), None),
    }
}

/// `hasDiffContent`
fn has_diff_content(
    file_diff: Option<&FileDiffMetadata>,
    old_file: Option<&FileContents>,
    new_file: Option<&FileContents>,
) -> bool {
    file_diff.is_some_and(|diff| !diff.hunks.is_empty()) || old_file.is_some() || new_file.is_some()
}

impl<Host, Rendered, LAnnotation> Lifecycle<'_, Host, Rendered, LAnnotation>
where
    Host: FileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    fn parse_diff_options(&self) -> crate::types::patch::ParseDiffOptions {
        self.options
            .borrow()
            .base
            .parse_diff_options
            .clone()
            .unwrap_or_default()
    }

    pub fn render(&self, props: FileDiffRenderProps<LAnnotation>) -> bool {
        assert!(
            self.state.borrow().enabled,
            "FileDiff.render: attempting to call render after cleaned up"
        );
        let FileDiffRenderProps {
            file_diff,
            defer_managers,
            force_render,
            prevent_emit,
            line_annotations,
            file_container,
            container_wrapper,
            render_range,
            old_file,
            new_file,
        } = props;
        let force_render = {
            let mut state = self.state.borrow_mut();
            std::mem::take(&mut state.force_render_override) || force_render
        };

        let file_input = get_diff_file_input(old_file, new_file, "FileDiff.render");
        let (old_file, new_file) = input_files(file_input.as_ref());

        let (collapsed, theme_type, expand_unchanged) = {
            let options = self.options.borrow();
            (
                options.base.base.collapsed.unwrap_or(false),
                options.base.base.theme_type.unwrap_or(ThemeTypes::System),
                options.base.expand_unchanged.unwrap_or(false),
            )
        };
        let next_render_range = if collapsed { None } else { render_range };
        let theme_changed = Theme(self.0).has_theme_changed();
        let has_file_input = file_input.is_some();
        let files_did_change = has_file_input && {
            let state = self.state.borrow();
            !are_files_equal(old_file, state.deletion_file.as_ref())
                || !are_files_equal(new_file, state.addition_file.as_ref())
        };
        let mut diff_did_change = file_diff.as_ref().is_some_and(|diff| {
            !are_diff_targets_equal(Some(&**diff), self.get_input_diff().as_deref())
        });
        let annotations_changed = match &line_annotations {
            Some(annotations)
                if !annotations.is_empty() || !self.get_latest_annotations().is_empty() =>
            {
                self.is_new_annotations(annotations)
            }
            _ => false,
        };

        if !collapsed
            && are_render_ranges_equal(
                next_render_range.as_ref(),
                self.state.borrow().render_range.as_ref(),
            )
            && !force_render
            && !annotations_changed
            && !theme_changed
            // If using the fileDiff API, lets check to see if they are equal to
            // avoid doing work
            && ((file_diff.is_some() && !diff_did_change)
                // If using the oldFile/newFile API then lets check to see if they are
                // equal
                || (file_diff.is_none() && !files_did_change))
        {
            return Theme(self.0).apply_cached_theme_state(theme_type);
        }

        let next_parsed_file_diff = (file_diff.is_none()
            && has_file_input
            && (files_did_change || self.get_input_diff().is_none()))
        .then(|| {
            parse_input(
                file_input.as_ref().expect("has_file_input"),
                &self.parse_diff_options(),
            )
        });

        let previous_render_range = {
            let mut state = self.state.borrow_mut();
            let previous = state.render_range;
            state.render_range = next_render_range;
            // Store files only when this render actually carried a file input:
            // internal rerenders pass none, and wiping the pair here would defeat the
            // oldFile/newFile early-return on every later host render. An explicit
            // fileDiff input supersedes a previously parsed pair, so clear it then.
            if has_file_input {
                state.deletion_file = old_file.cloned();
                state.addition_file = new_file.cloned();
            } else if file_diff.is_some() {
                state.deletion_file = None;
                state.addition_file = None;
            }
            previous
        };

        if let Some(file_diff) = file_diff.filter(|_| diff_did_change) {
            self.update_input_diff(file_diff, line_annotations.clone());
        } else if let Some(parsed) = next_parsed_file_diff {
            diff_did_change = true;
            self.update_input_diff(parsed, line_annotations.clone());
        }
        if diff_did_change {
            self.clear_reusable_header();
        }

        if let Some(line_annotations) = line_annotations {
            self.set_line_annotations(line_annotations);
        }

        let Some(working_diff) = self.get_working_diff(None) else {
            return false;
        };
        if expand_unchanged {
            Loading(self.0).load_files_if_necessary();
        }
        let renderer_options = get_diff_hunks_renderer_options(&self.options.borrow());
        self.hunks_renderer
            .borrow_mut()
            .set_options(renderer_options);
        self.sync_interaction_options();

        // `getDiffForNextRender(workingDiff)` is the working diff.
        self.prepare_render_data(&working_diff);

        let file_container = self.prepare_shell(file_container, container_wrapper, theme_type);

        if collapsed {
            self.render_collapsed(&working_diff, file_container, theme_type);
            if !prevent_emit {
                self.emit_post_render(false);
            }
            return true;
        }

        self.render_expanded(
            &working_diff,
            file_container,
            theme_type,
            next_render_range,
            previous_render_range,
            [
                force_render,
                annotations_changed,
                files_did_change || diff_did_change || theme_changed,
                defer_managers,
            ],
        );
        if !prevent_emit {
            self.emit_post_render(false);
        }
        true
    }

    /// The header removal, the container and the cached theme: what `render`
    /// does between choosing the diff and choosing collapsed or expanded.
    fn prepare_shell(
        &self,
        file_container: Option<NodeKey>,
        container_wrapper: Option<NodeKey>,
        theme_type: ThemeTypes,
    ) -> NodeKey {
        let (disable_file_header, header_element) = {
            let state = self.state.borrow();
            (
                self.options
                    .borrow()
                    .base
                    .base
                    .disable_file_header
                    .unwrap_or(false),
                state.header_element,
            )
        };
        if disable_file_header {
            // Remove existing header from DOM
            if let Some(header_element) = header_element {
                let mut state = self.state.borrow_mut();
                state.header_element = None;
                state.header_cache.last_rendered_header = None;
                drop(state);
                Nodes(self.0).drop_node(header_element);
            }
            Nodes(self.0).clear_header_slots();
        }
        let file_container =
            Nodes(self.0).get_or_create_file_container(file_container, container_wrapper);
        Theme(self.0).apply_cached_theme_state(theme_type);
        file_container
    }

    fn render_collapsed(
        &self,
        working_diff: &Rc<FileDiffMetadata>,
        file_container: NodeKey,
        theme_type: ThemeTypes,
    ) {
        let nodes = Nodes(self.0);
        nodes.remove_rendered_code();
        nodes.clear_auxiliary_nodes();

        let hunks_result = self.hunks_renderer.borrow().render_diff(
            &*self.highlighter,
            working_diff,
            &EMPTY_RENDER_RANGE,
        );
        Theme(self.0).apply_theme_state(
            file_container,
            &hunks_result.theme_styles,
            theme_type,
            hunks_result.base_theme_type,
        );
        if let Some(header_element) = &hunks_result.header_element {
            nodes.apply_header_to_dom(header_element, file_container, working_diff);
        }
        nodes.render_separators(&[]);
        self.adopt_render_data(Rc::clone(working_diff));
        Theme(self.0).inject_unsafe_css();
        // The collapsed render never sets the managers up again, and
        // `remove_rendered_code` cleaned all of them up: nothing holds a key of
        // the nodes this render retired.
        nodes.discard_retired();
    }

    /// `flags`: `forceRender`, `annotationsChanged`, `filesDidChange ||
    /// diffDidChange || themeChanged` and `deferManagers`.
    fn render_expanded(
        &self,
        working_diff: &Rc<FileDiffMetadata>,
        file_container: NodeKey,
        theme_type: ThemeTypes,
        next_render_range: Option<RenderRange>,
        previous_render_range: Option<RenderRange>,
        [
            force_render,
            annotations_changed,
            content_changed,
            defer_managers,
        ]: [bool; 4],
    ) {
        let nodes = Nodes(self.0);
        let pre = nodes.get_or_create_pre_node(file_container);

        // Attempt to partially render
        let did_partially_render = self.options.borrow().can_partially_render(
            force_render,
            annotations_changed,
            content_changed
                || !are_diff_targets_equal(
                    self.get_rendered_diff().as_deref(),
                    Some(&**working_diff),
                ),
        ) && Columns(self.0).apply_partial_render(
            working_diff,
            previous_render_range.as_ref(),
            next_render_range.as_ref(),
        );

        // If we were unable to partially render, perform a full render
        if did_partially_render {
            if let Some(rendered_diff) = self.get_rendered_diff() {
                self.adopt_render_data(rendered_diff);
            }
        } else {
            let hunks_result = self.hunks_renderer.borrow().render_diff(
                &*self.highlighter,
                working_diff,
                &next_render_range.unwrap_or(DEFAULT_RENDER_RANGE),
            );
            Theme(self.0).apply_theme_state(
                file_container,
                &hunks_result.theme_styles,
                theme_type,
                hunks_result.base_theme_type,
            );
            if let Some(header_element) = &hunks_result.header_element {
                nodes.apply_header_to_dom(header_element, file_container, working_diff);
            }
            if hunks_result.additions_content_ast.is_some()
                || hunks_result.deletions_content_ast.is_some()
                || hunks_result.unified_content_ast.is_some()
            {
                Columns(self.0).apply_hunks_to_dom(pre, &hunks_result);
            } else {
                let pre = self.state.borrow_mut().pre.take();
                nodes.drop_optional(pre);
            }
            nodes.render_separators(&hunks_result.hunk_data);
            self.adopt_render_data(Rc::clone(working_diff));
        }
        nodes.apply_buffers(pre, next_render_range.as_ref());
        Theme(self.0).inject_unsafe_css();
        nodes.render_annotations();
        nodes.render_gutter_utility();

        self.state.borrow_mut().managers_dirty = true;
        if !defer_managers {
            self.flush_managers();
        }
    }

    pub(super) fn emit_post_render(&self, unmount: bool) {
        let (file_container, on_post_render) = (
            self.state.borrow().file_container,
            self.options.borrow().on_post_render.clone(),
        );
        if unmount {
            {
                let mut state = self.state.borrow_mut();
                if !state.mounted {
                    return;
                }
                state.mounted = false;
            }
            let Some(file_container) = file_container else {
                return;
            };
            if let Some(on_post_render) = on_post_render {
                on_post_render(&file_container, PostRenderPhase::Unmount);
            }
            return;
        }

        let Some(file_container) = file_container else {
            return;
        };
        let phase = {
            let mut state = self.state.borrow_mut();
            let phase = if state.mounted {
                PostRenderPhase::Update
            } else {
                PostRenderPhase::Mount
            };
            state.mounted = true;
            phase
        };
        if let Some(on_post_render) = on_post_render {
            on_post_render(&file_container, phase);
        }
    }

    // Store a new input. Return false if the input has not changed. (The edit
    // session's outgoing diff and annotation adoption are dropped.)
    pub(super) fn update_input_diff(
        &self,
        incoming_diff: Rc<FileDiffMetadata>,
        line_annotations: Option<LineAnnotations<LAnnotation>>,
    ) -> bool {
        if are_diff_targets_equal(self.get_input_diff().as_deref(), Some(&*incoming_diff)) {
            return false;
        }
        let previous_annotations = self.get_latest_annotations();
        let mut state = self.state.borrow_mut();
        state.file_diff = Some(Rc::clone(&incoming_diff));
        state.pending_data = Some(FileDiffRenderData {
            file_diff: incoming_diff,
            line_annotations: line_annotations.unwrap_or(previous_annotations),
        });
        true
    }

    // Build rows using annotations for the diff the renderer selected.
    // Subclasses also prepare their additional rows here.
    pub(super) fn prepare_render_data(&self, file_diff: &FileDiffMetadata) {
        let annotations = self.get_annotations_for_diff(file_diff);
        self.hunks_renderer
            .borrow_mut()
            .set_line_annotations(annotations.to_vec());
    }

    // Record the diff now on screen. If it matches pendingData, apply those
    // annotations and clear pendingData; otherwise keep the pending update.
    pub(super) fn adopt_render_data(&self, file_diff: Rc<FileDiffMetadata>) {
        let line_annotations = self.line_annotations();
        let mut state = self.state.borrow_mut();
        let matching_pending = state.pending_data.take_if(|pending| {
            are_diff_targets_equal(Some(&*file_diff), Some(&*pending.file_diff))
        });
        state.rendered_data = Some(FileDiffRenderData {
            file_diff,
            line_annotations: matching_pending
                .map_or(line_annotations, |pending| pending.line_annotations),
        });
    }

    /// `hydrate`: `props.file_container` is required. `prerenderedHTML` is not
    /// supported (see the header).
    pub fn hydrate(&self, props: FileDiffRenderProps<LAnnotation>) {
        let file_container = props
            .file_container
            .expect("FileDiff.hydrate: a fileContainer is required");
        let prevent_emit = props.prevent_emit;
        {
            let state = self.state.borrow();
            assert!(
                state.enabled,
                "FileDiff.hydrate: attempting to call hydrate after cleaned up"
            );
            assert!(
                state.file_container.is_none(),
                "FileDiff.hydrate: hydrate can only be called before the instance has rendered or hydrated"
            );
        }
        let FileDiffRenderProps {
            file_diff,
            old_file,
            new_file,
            ..
        } = &props;
        let file_input =
            get_diff_file_input(old_file.clone(), new_file.clone(), "FileDiff.hydrate");
        let (old_file, new_file) = input_files(file_input.as_ref());
        self.hydrate_elements(file_container);
        let (pre, header_element) = {
            let state = self.state.borrow();
            (state.pre, state.header_element)
        };
        let (collapsed, disable_file_header) = {
            let options = self.options.borrow();
            (
                options.base.base.collapsed.unwrap_or(false),
                options.base.base.disable_file_header.unwrap_or(false),
            )
        };
        let has_header_content = file_diff.is_some() || old_file.is_some() || new_file.is_some();
        if (!collapsed
            && pre.is_none()
            && has_diff_content(file_diff.as_deref(), old_file, new_file))
            || (header_element.is_none() && has_header_content && !disable_file_header)
        {
            self.render(FileDiffRenderProps {
                file_container: Some(file_container),
                prevent_emit: true,
                file_diff: props.file_diff,
                line_annotations: props.line_annotations,
                defer_managers: props.defer_managers,
                force_render: props.force_render,
                container_wrapper: props.container_wrapper,
                render_range: props.render_range,
                old_file: props.old_file,
                new_file: props.new_file,
            });
        }
        // Otherwise orchestrate our setup
        else {
            self.hydration_setup(props.file_diff, file_input.as_ref(), props.line_annotations);
        }
        if !prevent_emit {
            self.emit_post_render(false);
        }
    }

    pub fn rerender(&self) {
        let (enabled, no_input) = {
            let state = self.state.borrow();
            (
                state.enabled,
                state.file_diff.is_none()
                    && state.addition_file.is_none()
                    && state.deletion_file.is_none(),
            )
        };
        if !enabled || no_input {
            return;
        }
        // `requestRender()`: an instance an owner drives asks the owner to render
        // (`forceRenderOverride`, then `requestOwnerRender`); the owner's next
        // `render` call carries the override. (Editors, which update their rows
        // directly, are dropped.)
        if let Some(request_owner_render) = &self.request_owner_render {
            self.state.borrow_mut().force_render_override = true;
            request_owner_render();
            return;
        }
        let render_range = self.state.borrow().render_range;
        self.render(FileDiffRenderProps {
            force_render: true,
            render_range,
            ..FileDiffRenderProps::default()
        });
    }

    fn hydrate_elements(&self, file_container: NodeKey) {
        if self.state.borrow().file_container != Some(file_container) {
            self.emit_post_render(true);
        }
        let document = self.document();
        if let Some(shadow_root) = document.shadow_root(file_container) {
            for element in document.children(shadow_root) {
                let tag = document.tag_name(element);
                let has = |name: &str| document.get_attribute(element, name).is_some();
                if tag == "SVG" {
                    self.state.borrow_mut().sprite_svg = Some(element);
                } else if tag == "PRE" {
                    self.state.borrow_mut().pre = Some(element);
                    for code in document.children(element) {
                        if document.tag_name(code) != "CODE" {
                            continue;
                        }
                        let mut state = self.state.borrow_mut();
                        if document.get_attribute(code, "data-deletions").is_some() {
                            state.code_deletions = Some(code);
                        }
                        if document.get_attribute(code, "data-additions").is_some() {
                            state.code_additions = Some(code);
                        }
                        if document.get_attribute(code, "data-unified").is_some() {
                            state.code_unified = Some(code);
                        }
                    }
                } else if has("data-diffs-header") {
                    self.state.borrow_mut().header_element = Some(element);
                } else if tag == "STYLE" && has(THEME_CSS_ATTRIBUTE) {
                    self.state.borrow_mut().theme_css_style = Some(element);
                } else if tag == "STYLE" && has(UNSAFE_CSS_ATTRIBUTE) {
                    let mut state = self.state.borrow_mut();
                    state.unsafe_css_style = Some(element);
                    state.applied_unsafe_css = Some(document.text_content(element));
                }
            }
        }
        let pre = self.state.borrow().pre;
        if let Some(pre) = pre {
            Nodes(self.0).sync_code_nodes_from_pre(pre);
            document.remove_attribute(pre, "data-dehydrated");
        }
        self.state.borrow_mut().file_container = Some(file_container);
        Theme(self.0).hydrate_measured_scrollbar();
    }

    fn hydration_setup(
        &self,
        file_diff: Option<Rc<FileDiffMetadata>>,
        file_input: Option<&DiffFileInput>,
        line_annotations: Option<LineAnnotations<LAnnotation>>,
    ) {
        // It's possible we are hydrating a pure-rename and therefore there will be
        // no pre element
        let hydrated_annotations =
            line_annotations.unwrap_or_else(|| self.get_latest_annotations());
        let (old_file, new_file) = input_files(file_input);
        let parsed = match (&file_diff, file_input) {
            (None, Some(input)) => Some(parse_input(input, &self.parse_diff_options())),
            _ => None,
        };
        let file_diff = file_diff.or(parsed);
        {
            let mut state = self.state.borrow_mut();
            state.pending_data = None;
            state.addition_file = new_file.cloned();
            state.deletion_file = old_file.cloned();
            state.file_diff = file_diff.clone();
            state.rendered_data = file_diff.clone().map(|file_diff| FileDiffRenderData {
                file_diff,
                line_annotations: hydrated_annotations,
            });
        }
        if let Some(file_diff) = file_diff {
            self.adopt_render_data(file_diff);
        }

        if self.state.borrow().pre.is_none() {
            return;
        }

        self.sync_interaction_options();
        // `hunksRenderer.hydrate(fileDiff)` has no render cache to seed.
        // FIXME(amadeus): not sure how to handle this yet...
        // this.renderSeparators();
        Nodes(self.0).render_annotations();
        Nodes(self.0).render_gutter_utility();
        Theme(self.0).inject_unsafe_css();
        self.state.borrow_mut().managers_dirty = true;
        self.flush_managers();
    }
}
