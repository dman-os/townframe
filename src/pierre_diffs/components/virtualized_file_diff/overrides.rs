// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/VirtualizedFileDiff.ts
// (S12b-3: the integration with `FileDiff`; upstream lines 185-195
// (`setLineAnnotations`), 264-308 (`setOptions`, `setThemeType`), 540-559
// (`onRender`, `flushManagers`), 952-1070 (`cleanUp`, `expandHunk`,
// `handleFilesLoaded`, `loadFilesIfNecessary`), 1071-1112 (`isLineRenderable`),
// 1151-1192 (`setVisibility`, `rerender`), 1396-1549 (`render`,
// `finalizeRender`)).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * The methods `FileDiff` itself calls (`finalizeRender`, `expandHunk`,
//   `handleFilesLoaded`, `setLineAnnotations`, `flushManagers`' tail) are the
//   `FileDiffHooks` of `OverrideHooks`, which holds a `Weak` of the instance
//   (see `FileDiffHooks` in file_diff.rs); the methods only outside callers
//   invoke (`render`, `rerender`, `setOptions`, `setThemeType`, `cleanUp`,
//   `isLineRenderable`, `revealLine`, `onRender`, `setVisibility`) are methods
//   of the instance that call the `FileDiff` views in upstream's order.
//   `onRender` and `setVisibility` are the `SubscribedInstance` the virtualizer
//   drives: they call `render()` and `rerender()` themselves.
// * Dropped with the advanced (CodeView) mode: every `isAdvancedMode()` arm
//   (`setOptions` and `setThemeType` throw there, `setVisibility` returns,
//   `render` sets `isVisible` and skips the connection, `expandHunk` and
//   `handleFilesLoaded` stage `pendingExpansions` and `pendingHydratedDiff`),
//   hence also `loadFilesIfNecessary` (it only clears a `pendingHydratedDiff`
//   that is never set, then calls the base class), `isLineRenderable` (its
//   extra arm reads the never-set `pendingExpansions` and returns `false`), the
//   `isComponentManaged` getter (CodeView's, else the base class's),
//   `shouldDisableVirtualizationBuffers` (`isAdvancedMode() || base`) and
//   `shouldGuardRebuildScroll` (the WebKit guard is dropped in `columns.rs`):
//   what remains is the base class's behaviour, so `FileDiffHooks` has no such
//   methods and `is_line_renderable` forwards. `cleanUp`'s `shouldRecomputeLayout`
//   and ghost text arms are dropped with CodeView and the editor.
// * `handleFilesLoaded` is `FileDiff`'s merge (`Loading::merge_loaded_files`)
//   followed by this class's resets. The pending render's diffs are swapped for
//   the hydrated diff as the instance's other `Rc`s are (one object whose
//   `isPartial` turned false upstream is one `Rc` replaced everywhere here); the
//   `awaitWithTimeout(primeHighlightCache)` is dropped with the worker pool, and
//   `installHydratedSessionDiff` with the edit session.
// * Errors are programming errors and panic: `render` without a diff
//   (`console.error` and `return false` upstream: a caller that renders before
//   it has given the instance anything) and `finalizeRender`'s throw.
// * `render` is the same function of its props: `...fileInput` and
//   `...fileInputProps` follow the computed props in the call to `super.render`,
//   so a `renderRange` the caller passed replaces the computed one
//   (`props.render_range.or(computed)`).
// * `render` stores `fileInput`'s files in the placeholder branch through
//   `FileDiff::virtualized().set_files` (a clone, as `FileDiff::render`'s).

use std::rc::Rc;
use std::rc::Weak;

use super::instance::VirtualizedFileDiff;
use super::layout::ResetLayoutCacheOptions;
use super::layout::has_diff_estimate_option_changed;
use super::layout::has_diff_layout_option_changed;
use crate::components::file_diff::FileDiffHooks;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::FileDiffRenderProps;
use crate::components::file_diff::LineAnnotations;
use crate::components::file_diff::RenderedContent;
use crate::components::file_diff::VirtualizedFileDiffHost;
use crate::components::virtualizer::SubscribedInstance;
use crate::document::NodeKey;
use crate::types::layout::RenderWindow;
use crate::types::lines::ExpansionDirections;
use crate::types::options::ThemeTypes;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::FileDiffMetadata;
use crate::utils::options::get_diff_file_input::get_diff_file_input;
use crate::utils::predicates::are_diff_targets_equal::are_diff_targets_equal;
use crate::utils::predicates::are_options_equal::are_options_equal;

/// The `FileDiffHooks` of a `VirtualizedFileDiff`: the methods it overrides
/// that `FileDiff` calls from inside its own methods.
pub(super) struct OverrideHooks<
    Host: VirtualizedFileDiffHost,
    Rendered: 'static,
    LAnnotation: 'static,
> {
    pub(super) instance: Weak<VirtualizedFileDiff<Host, Rendered, LAnnotation>>,
}

impl<Host, Rendered, LAnnotation> OverrideHooks<Host, Rendered, LAnnotation>
where
    Host: VirtualizedFileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    fn instance(&self) -> Rc<VirtualizedFileDiff<Host, Rendered, LAnnotation>> {
        self.instance
            .upgrade()
            .expect("the instance outlives the FileDiff it extends")
    }
}

impl<Host, Rendered, LAnnotation> FileDiffHooks<Host, Rendered, LAnnotation>
    for OverrideHooks<Host, Rendered, LAnnotation>
where
    Host: VirtualizedFileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    fn finalize_render(
        &self,
        _file_diff: &crate::components::file_diff::FileDiff<Host, Rendered, LAnnotation>,
    ) {
        self.instance().finalize_render();
    }

    fn expand_hunk(
        &self,
        _file_diff: &crate::components::file_diff::FileDiff<Host, Rendered, LAnnotation>,
        hunk_index: i64,
        direction: ExpansionDirections,
        expansion_line_count_override: Option<f64>,
    ) {
        self.instance()
            .expand_hunk(hunk_index, direction, expansion_line_count_override);
    }

    fn handle_files_loaded(
        &self,
        _file_diff: &crate::components::file_diff::FileDiff<Host, Rendered, LAnnotation>,
        expected_diff: &Rc<FileDiffMetadata>,
        files: &FileDiffLoadedFiles,
    ) {
        self.instance().handle_files_loaded(expected_diff, files);
    }

    fn set_line_annotations(
        &self,
        _file_diff: &crate::components::file_diff::FileDiff<Host, Rendered, LAnnotation>,
        line_annotations: LineAnnotations<LAnnotation>,
    ) {
        self.instance().set_line_annotations(line_annotations);
    }

    fn flush_managers_done(
        &self,
        file_diff: &crate::components::file_diff::FileDiff<Host, Rendered, LAnnotation>,
    ) {
        // Virtualized renders bypass FileDiff's queued render handler. Apply line
        // state deferred during the refresh once managers target the rebuilt rows.
        if file_diff.virtualized().line_state_refresh_pending() {
            file_diff.flush_deferred_line_state();
        }
    }
}

impl<Host, Rendered, LAnnotation> SubscribedInstance
    for VirtualizedFileDiff<Host, Rendered, LAnnotation>
where
    Host: VirtualizedFileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    fn on_render(&self, dirty: bool) -> bool {
        if self.file_diff().virtualized().file_container().is_none() {
            return false;
        }
        if dirty {
            self.sync_virtualized_top();
        }
        self.render(FileDiffRenderProps::default())
    }

    fn reconcile_heights(&self) -> bool {
        VirtualizedFileDiff::reconcile_heights(self)
    }

    fn set_visibility(&self, visible: bool) {
        let file_diff = self.file_diff();
        if file_diff.virtualized().file_container().is_none() {
            return;
        }
        file_diff.virtualized().set_render_range(None);
        let is_visible = self.is_visible();
        if visible && !is_visible {
            self.sync_virtualized_top();
            self.set_is_visible(true);
        } else if !visible && is_visible {
            self.set_is_visible(false);
            self.rerender();
        }
    }
}

impl<Host, Rendered, LAnnotation> VirtualizedFileDiff<Host, Rendered, LAnnotation>
where
    Host: VirtualizedFileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    pub fn set_line_annotations(&self, line_annotations: LineAnnotations<LAnnotation>) {
        if self.sync_line_annotations(Some(&line_annotations)) {
            self.file_diff().virtualized().force_render_override();
        }
    }

    pub fn set_options(&self, options: Option<FileDiffOptions<NodeKey, Rendered, LAnnotation>>) {
        let Some(options) = options else { return };
        let (options_changed, layout_changed, estimate_changed) = {
            let previous_options = self.file_diff().options();
            let options_changed = !are_options_equal(Some(&*previous_options), Some(&options));
            let layout_changed =
                options_changed && has_diff_layout_option_changed(&previous_options, &options);
            (
                options_changed,
                layout_changed,
                layout_changed && has_diff_estimate_option_changed(&previous_options, &options),
            )
        };

        self.file_diff().set_options(Some(options));

        if layout_changed {
            self.reset_layout_cache(ResetLayoutCacheOptions {
                force_simple_recompute: true,
                include_estimated_heights: estimate_changed,
                ..ResetLayoutCacheOptions::default()
            });
        }
        // Any option can affect rendered DOM; only layout-affecting options clear
        // the measured height cache above.
        if options_changed {
            self.file_diff().virtualized().force_render_override();
            self.virtualizer()
                .instance_changed(&self.instance_ref(), layout_changed);
        }
    }

    pub fn set_theme_type(&self, theme_type: ThemeTypes) {
        self.file_diff().set_theme_type(theme_type);
    }

    pub fn is_line_renderable(&self, line_number: usize) -> bool {
        self.file_diff().lines().is_line_renderable(line_number)
    }

    pub fn reveal_line(&self, line_number: usize) -> bool {
        self.file_diff().lines().reveal_line(line_number)
    }

    pub fn expand_hunk(
        &self,
        hunk_index: i64,
        direction: ExpansionDirections,
        expansion_line_count_override: Option<f64>,
    ) {
        let file_diff = self.file_diff();
        if file_diff.get_input_diff().is_none() {
            return;
        }
        file_diff.loading().load_files_if_necessary();
        file_diff.virtualized().expand_renderer_hunk(
            usize::try_from(hunk_index).expect("hunk index is not negative"),
            direction,
            expansion_line_count_override.map(|count| {
                if count.is_infinite() {
                    usize::MAX
                } else {
                    count as usize
                }
            }),
        );
        self.reset_layout_cache(ResetLayoutCacheOptions {
            include_estimated_heights: true,
            ..ResetLayoutCacheOptions::default()
        });
        self.compute_approximate_size(false, None);
        file_diff.virtualized().force_render_override();
        self.virtualizer()
            .instance_changed(&self.instance_ref(), true);
    }

    fn handle_files_loaded(
        &self,
        expected_diff: &Rc<FileDiffMetadata>,
        files: &FileDiffLoadedFiles,
    ) {
        let file_diff = self.file_diff();
        if file_diff
            .get_input_diff()
            .is_none_or(|input| !Rc::ptr_eq(&input, expected_diff))
            || !expected_diff.is_partial
        {
            return;
        }
        let hydrated = file_diff.loading().merge_loaded_files(expected_diff, files);
        self.swap_pending_render_diff(expected_diff, &hydrated);
        self.reset_layout_cache(ResetLayoutCacheOptions {
            include_estimated_heights: true,
            ..ResetLayoutCacheOptions::default()
        });
        self.compute_approximate_size(false, None);
        file_diff.virtualized().force_render_override();
        self.virtualizer()
            .instance_changed(&self.instance_ref(), true);
    }

    /// `render`: what `onRender` calls, and the outside callers' entry point.
    pub fn render(&self, props: FileDiffRenderProps<LAnnotation>) -> bool {
        let FileDiffRenderProps {
            file_diff: given_diff,
            defer_managers,
            force_render,
            prevent_emit,
            line_annotations,
            file_container,
            container_wrapper,
            render_range: given_render_range,
            old_file,
            new_file,
        } = props;
        let inner = self.file_diff();
        let virtualized = inner.virtualized();
        let file_input = get_diff_file_input(old_file, new_file, "VirtualizedFileDiff.render");
        let (input_old, input_new) = file_input
            .as_ref()
            .map_or((None, None), |input| input.files());
        let files_did_change =
            file_input.is_some() && virtualized.files_differ(input_old, input_new);
        let mut next_file_diff = given_diff.clone().or_else(|| inner.get_input_diff());
        if let Some(file_input) = file_input.as_ref().filter(|_| {
            given_diff.is_none() && (files_did_change || inner.get_input_diff().is_none())
        }) {
            next_file_diff = Some(virtualized.parse_input(file_input));
        }
        let force_render_override = virtualized.take_force_render_override();
        let is_setup = self.is_setup();
        let target_changed = next_file_diff.as_ref().is_some_and(|next| {
            !are_diff_targets_equal(inner.get_input_diff().as_deref(), Some(&**next))
        });
        let data_changed = target_changed || files_did_change;

        let file_container = virtualized.get_or_create_file_container(file_container);

        let Some(next_file_diff) = next_file_diff else {
            panic!(
                "VirtualizedFileDiff.render: attempting to virtually render when we dont have the correct data"
            );
        };
        if target_changed {
            virtualized.update_input_diff(Rc::clone(&next_file_diff), line_annotations.clone());
        }

        let (pending_render_diff, layout_diff_changed, rendered_diff_changed, annotations_changed) =
            match self.pending_render_for(&next_file_diff) {
                Some(diff) => (
                    diff,
                    false,
                    false,
                    self.sync_line_annotations(line_annotations.as_ref()),
                ),
                None => {
                    let update =
                        self.update_pending_render(&next_file_diff, line_annotations.as_ref());
                    (
                        update.pending_render_diff,
                        update.layout_diff_changed,
                        update.rendered_diff_changed,
                        update.annotations_changed,
                    )
                }
            };

        if layout_diff_changed {
            self.reset_layout_cache(ResetLayoutCacheOptions {
                include_estimated_heights: true,
                ..ResetLayoutCacheOptions::default()
            });
        }

        if is_setup {
            self.ensure_top();
            if self.layout().layout_dirty {
                self.virtualizer().mark_dom_dirty();
                self.compute_approximate_size(false, Some(&pending_render_diff));
            }
        } else {
            self.compute_approximate_size(false, Some(&pending_render_diff));
            self.ensure_top();
            // Upstream drops the disposer `connect` returns.
            drop(
                self.virtualizer()
                    .connect(file_container, self.instance_ref()),
            );
            let (top, height) = (self.top().unwrap_or(0.0), self.get_virtualized_height());
            self.set_is_visible(self.virtualizer().is_instance_visible(top, height));
            self.set_is_setup(true);
        }

        if !self.is_visible() && (!data_changed || !is_setup) {
            if file_input.is_some() {
                virtualized.set_files(input_old, input_new);
            }
            if target_changed {
                inner.clear_reusable_header();
            }
            return virtualized.render_placeholder(self.get_virtualized_height());
        }

        let window_specs = self.virtualizer().get_window_specs();
        let file_top = self.top().unwrap_or(0.0);
        let render_range = self.compute_render_range_from_window(
            &pending_render_diff,
            file_top,
            RenderWindow {
                top: window_specs.top,
                bottom: window_specs.bottom,
            },
        );
        let (old_file, new_file) =
            file_input.map_or((None, None), DiffFileInput::into_render_props);
        inner.lifecycle().render(FileDiffRenderProps {
            file_diff: Some(next_file_diff),
            file_container: Some(file_container),
            container_wrapper,
            // `...fileInputProps` follows `renderRange` in the call upstream.
            render_range: given_render_range.or(Some(render_range)),
            line_annotations,
            force_render: force_render_override
                || force_render
                || annotations_changed
                || rendered_diff_changed
                || target_changed,
            defer_managers,
            prevent_emit,
            old_file,
            new_file,
        })
    }

    /// `finalizeRender`
    fn finalize_render(&self) {
        let rendered_diff = self.file_diff().get_rendered_diff();
        assert!(
            are_diff_targets_equal(
                rendered_diff.as_deref(),
                self.pending_render_diff().as_deref()
            ),
            "VirtualizedFileDiff.render: rendered a different diff than its prepared layout"
        );
        self.clear_pending_render();
        // Renders can be driven from outside the virtualizer (host/React render
        // calls, async highlight completions), and the virtualizer only
        // auto-reconciles renders it initiated. Queue a measured-height
        // reconciliation for every applied content render so line deltas
        // (wrapped lines, annotation heights) survive layout resets.
        self.virtualizer()
            .request_height_reconcile(&self.instance_ref());
    }

    pub fn rerender(&self) {
        let file_diff = self.file_diff();
        let virtualized = file_diff.virtualized();
        if !virtualized.can_rerender() {
            return;
        }
        // `getDiffForNextRender(workingDiff)` is the working diff.
        let next_render_diff = file_diff.get_working_diff(None);
        // A completed async highlight can change which diff the next simple
        // virtualizer render will commit.
        self.clear_pending_render();
        virtualized.force_render_override();
        if virtualized.request_render() {
            return;
        }
        self.virtualizer().instance_changed(
            &self.instance_ref(),
            !are_diff_targets_equal(
                file_diff.get_rendered_diff().as_deref(),
                next_render_diff.as_deref(),
            ),
        );
    }

    pub fn clean_up(&self, recycle: bool) {
        let file_diff = self.file_diff();
        if let Some(file_container) = file_diff.virtualized().file_container() {
            self.virtualizer().disconnect(&file_container);
        }
        if !recycle {
            self.reset_layout_cache(ResetLayoutCacheOptions {
                include_estimated_heights: true,
                ..ResetLayoutCacheOptions::default()
            });
        }
        self.clear_pending_render();
        self.set_is_setup(false);
        file_diff.lifecycle().clean_up(recycle);
    }
}
