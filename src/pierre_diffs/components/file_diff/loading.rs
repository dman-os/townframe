// Part of the S12a-2b port of FileDiff.ts (see ../file_diff.rs for the header):
// loading the full files of a partial diff: `loadFilesIfNecessary`,
// `loadFilesForDiff`/`handleFilesLoaded` (the half after the load resolves) and
// `setHydratedState`. A view over the instance (see `file_diff_view!`).
// Changes (besides the ones in the main header):
// * Loading is host-driven. `load_files_if_necessary` records the diff a load
//   is wanted for (`pending_files`) and the host, which reads it from
//   `FileDiff::pending_files`, calls `loadDiffFiles(fileDiff)` and then
//   `FileDiff::files_loaded` (the `.then` of `loadFilesForDiff`). When the load
//   fails, the host calls `FileDiff::abandon_file_load` (upstream's `finally`
//   clears the pending request; its `catch` logs the error, or rethrows under
//   `disableErrorHandling`, which is the host's call here).
// * `hydratePartialDiff('merge', ...)` mutates the diff in place upstream. The
//   diffs are `Rc`s, so the merge hydrates a copy and the instance swaps the old
//   `Rc` for it wherever it held it (the input, the rendered data, the pending
//   data): what upstream observes as one object whose `isPartial` turned false is
//   one `Rc` replaced everywhere in the instance. A host holding the old `Rc`
//   sees it unchanged.
// * `awaitWithTimeout(() => this.primeHighlightCache(...))` is dropped with the
//   worker pool and the async highlight cache: rendering is synchronous. The
//   `!this.enabled || getInputDiff() !== expectedDiff` re-check after it is
//   dropped with the await (nothing runs in between).
// * `installHydratedSessionDiff` is edit-session code and is dropped.

use std::rc::Rc;

use super::*;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::ParseDiffOptions;
use crate::utils::hydrate_partial_diff::hydrate_partial_diff_clone;

file_diff_view!(Loading);

/// `canHydrateDiff`
fn can_hydrate_diff(file_diff: &FileDiffMetadata) -> bool {
    file_diff.is_partial
        && matches!(
            file_diff.change_type,
            ChangeTypes::Change | ChangeTypes::RenameChanged | ChangeTypes::RenamePure
        )
}

impl<Host, Rendered, LAnnotation> Loading<'_, Host, Rendered, LAnnotation>
where
    Host: FileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    pub(super) fn load_files_if_necessary(&self) {
        let Some(file_diff) = self.get_input_diff() else {
            return;
        };
        let mut state = self.state.borrow_mut();
        if !can_hydrate_diff(&file_diff)
            || state
                .pending_files
                .as_ref()
                .is_some_and(|pending| Rc::ptr_eq(pending, &file_diff))
        {
            return;
        }
        assert!(
            self.options.borrow().has_diff_file_loader,
            "FileDiff: loadDiffFiles is required to load full files for a partial diff"
        );
        state.pending_files = Some(file_diff);
    }

    /// The half of `loadFilesForDiff` after `loadDiffFiles` resolved, with the
    /// `finally` that clears the request.
    pub(super) fn files_loaded(
        &self,
        expected_diff: &Rc<FileDiffMetadata>,
        files: &FileDiffLoadedFiles,
    ) {
        let still_input = self
            .get_input_diff()
            .is_some_and(|input| Rc::ptr_eq(&input, expected_diff));
        if still_input {
            self.handle_files_loaded(expected_diff, files);
        }
        self.abandon_file_load(expected_diff);
    }

    pub(super) fn abandon_file_load(&self, expected_diff: &Rc<FileDiffMetadata>) {
        let mut state = self.state.borrow_mut();
        if state
            .pending_files
            .as_ref()
            .is_some_and(|pending| Rc::ptr_eq(pending, expected_diff))
        {
            state.pending_files = None;
        }
    }

    fn handle_files_loaded(
        &self,
        expected_diff: &Rc<FileDiffMetadata>,
        files: &FileDiffLoadedFiles,
    ) {
        if !expected_diff.is_partial {
            return;
        }
        let parse_diff_options: ParseDiffOptions = self
            .options
            .borrow()
            .base
            .parse_diff_options
            .clone()
            .unwrap_or_default();
        let hydrated = Rc::new(hydrate_partial_diff_clone(
            expected_diff,
            files,
            &parse_diff_options,
        ));
        {
            let mut state = self.state.borrow_mut();
            let swap = |slot: &mut Rc<FileDiffMetadata>| {
                if Rc::ptr_eq(slot, expected_diff) {
                    *slot = Rc::clone(&hydrated);
                }
            };
            if let Some(file_diff) = state.file_diff.as_mut() {
                swap(file_diff);
            }
            if let Some(rendered) = state.rendered_data.as_mut() {
                swap(&mut rendered.file_diff);
            }
            if let Some(pending) = state.pending_data.as_mut() {
                swap(&mut pending.file_diff);
            }
            // `setHydratedState(files)`: `clearRenderCache` has no cache to clear.
            match files {
                FileDiffLoadedFiles::Changed { old_file, new_file } => {
                    state.deletion_file = Some(old_file.clone());
                    state.addition_file = Some(new_file.clone());
                }
                FileDiffLoadedFiles::PureRenamed { new_file } => {
                    state.deletion_file = None;
                    state.addition_file = Some(new_file.clone());
                }
            }
        }
        self.rerender();
    }
}
