// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C2: `createDiffOptionsPrototype`, `createDiffOptions`, `defineItemOption`,
// `defineItemSharedCallback`, `defineItemSelectionCallback`, `getItemOptions`,
// `updateItemOptionsId`, 2509-2786).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: (closure notes, design point 2 and Q6)
// * Upstream hands each instance an options object whose getters read the live
//   CodeView options. `FileDiff` owns its `FileDiffOptions` here, so CodeView
//   builds the item's options from its current options and the item
//   (`build_diff_options`) and pushes them to the instance
//   (`push_item_options`, which skips the push when `areOptionsEqual` holds)
//   at exactly the two places upstream reads them lazily: before
//   `updateCodeViewLayout` and, in C5, before a render whose
//   `renderedOptionsRevision` differs.
// * The callback wrappers are made once per item and key (`ItemCallbackCache`,
//   lazily, as `callbackCache` is). Each resolves the item by its current id
//   (`ItemOptionsState`) at call time and does nothing when the item is gone
//   or its type differs, then calls CodeView's current callback.
//   The user callbacks (`CodeViewOptions.callbacks`) receive the item context
//   (`CodeViewDiffItemContext`) as their trailing argument, as upstream's do:
//   the wrapper hands the item as it is when the callback runs (a snapshot:
//   no borrow of the view's state is held across a user callback).
// * The selection wrappers (`onLineSelected`, `onLineSelectionStart`,
//   `onLineSelectionChange`, `onLineSelectionEnd`) are always installed, as
//   upstream's accessors always return one.
// * `onEditComplete` and `collapsed`'s undefined-when-stateless arms are
//   dropped (edit mode; the push always has the item).

use std::rc::Rc;
use std::rc::Weak;

use super::CodeView;
use super::CodeViewHost;
use super::options::CodeViewCallbacks;
use super::options::CodeViewDiffItemContext;
use super::options::ItemSelectionCallback;
use super::state::CodeViewLineSelection;
use super::state::ItemKind;
use super::state::ItemOptionsState;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::OnPostRenderCallback;
use crate::components::file_diff::RenderAnnotationCallback;
use crate::components::file_diff::RenderGutterUtilityCallback;
use crate::components::file_diff::RenderHeaderCallback;
use crate::components::file_diff::RenderedContent;
use crate::document::NodeKey;
use crate::js_collections::PtrEq;
use crate::managers::interaction_manager::GetHoveredLineResult;
use crate::managers::interaction_manager::LineCallback;
use crate::managers::interaction_manager::LineEventProps;
use crate::managers::interaction_manager::SelectionCallback;
use crate::managers::interaction_manager::SelectionWriteOptions;
use crate::types::options::PostRenderPhase;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectedLineRange;

/// Reads one of the four user selection callbacks out of the callbacks.
type SelectCallback<Rendered, LAnnotation> =
    fn(&CodeViewCallbacks<Rendered, LAnnotation>) -> Option<ItemSelectionCallback<LAnnotation>>;

/// `CodeViewItemCallbackCache`: the wrappers an item has handed out so far.
pub struct ItemCallbackCache<Rendered, LAnnotation> {
    render_custom_header: Option<RenderHeaderCallback<Rendered>>,
    render_header_prefix: Option<RenderHeaderCallback<Rendered>>,
    render_header_filename_suffix: Option<RenderHeaderCallback<Rendered>>,
    render_header_metadata: Option<RenderHeaderCallback<Rendered>>,
    render_annotation: Option<RenderAnnotationCallback<Rendered, LAnnotation>>,
    render_gutter_utility: Option<RenderGutterUtilityCallback<Rendered>>,
    on_post_render: Option<OnPostRenderCallback<NodeKey>>,
    on_gutter_utility_click: Option<Rc<dyn Fn(SelectedLineRange)>>,
    on_line_click: Option<LineCallback<NodeKey>>,
    on_line_number_click: Option<LineCallback<NodeKey>>,
    on_line_enter: Option<LineCallback<NodeKey>>,
    on_line_leave: Option<LineCallback<NodeKey>>,
    on_line_selected: Option<SelectionCallback>,
    on_line_selection_start: Option<SelectionCallback>,
    on_line_selection_change: Option<SelectionCallback>,
    on_line_selection_end: Option<SelectionCallback>,
}

impl<Rendered, LAnnotation> Default for ItemCallbackCache<Rendered, LAnnotation> {
    fn default() -> Self {
        Self {
            render_custom_header: None,
            render_header_prefix: None,
            render_header_filename_suffix: None,
            render_header_metadata: None,
            render_annotation: None,
            render_gutter_utility: None,
            on_post_render: None,
            on_gutter_utility_click: None,
            on_line_click: None,
            on_line_number_click: None,
            on_line_enter: None,
            on_line_leave: None,
            on_line_selected: None,
            on_line_selection_start: None,
            on_line_selection_change: None,
            on_line_selection_end: None,
        }
    }
}

/// `defineItemSharedCallback`'s wrapper: a closure that runs CodeView's
/// current callback `$select` of `options.callbacks` with the item context
/// appended to its arguments (`$pass` is what each parameter hands over: the
/// `FileDiff` callback's argument, by reference where the user callback takes
/// it so), when the item is still registered, and returns the return type's
/// default otherwise.
macro_rules! shared_wrapper {
    ($code_view:ident, $state:ident, ($($arg:ident: $ty:ty => $pass:expr),*) -> $ret:ty, $select:ident) => {{
        let weak = Weak::clone(&$code_view.weak_self);
        let state = Rc::clone($state);
        move |$($arg: $ty),*| -> $ret {
            let code_view = weak
                .upgrade()
                .expect("the CodeView outlives its items' callbacks");
            let Some(context) = code_view.item_context(&state) else {
                return Default::default();
            };
            let callback = code_view.state.borrow().options.callbacks.$select.clone();
            match callback {
                Some(callback) => (*callback)($($pass,)* &context),
                None => Default::default(),
            }
        }
    }};
}

impl<Host, Rendered, LAnnotation> CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `getItemOptions(state, 'diff')`: the item the state names, when it is
    /// still registered as a diff, as the context a callback receives.
    pub(super) fn item_context(
        &self,
        state: &ItemOptionsState,
    ) -> Option<CodeViewDiffItemContext<LAnnotation>> {
        let id = state.id.borrow();
        let view = self.state.borrow();
        let item = view.id_to_item.get(id.as_str())?.borrow();
        let ItemKind::Diff(diff) = &item.kind else {
            return None;
        };
        Some(CodeViewDiffItemContext {
            item: diff.item.clone(),
            index: item.index,
            top: item.top,
            height: item.height,
            element: item.element,
            version: item.version,
            rendered_options_revision: item.rendered_options_revision,
        })
    }

    /// `defineItemSelectionCallback`'s wrapper for the callback `select` reads
    /// from the options.
    fn selection_wrapper(
        &self,
        state: &Rc<ItemOptionsState>,
        select: SelectCallback<Rendered, LAnnotation>,
    ) -> SelectionCallback {
        let weak = Weak::clone(&self.weak_self);
        let state = Rc::clone(state);
        Rc::new(move |range: Option<&SelectedLineRange>| {
            let code_view = weak
                .upgrade()
                .expect("the CodeView outlives its items' callbacks");
            let Some(context) = code_view.item_context(&state) else {
                return;
            };
            let id = &context.item.id;
            let selection = range.map(|range| CodeViewLineSelection {
                id: id.clone(),
                range: range.clone(),
            });
            let controlled = code_view
                .state
                .borrow()
                .options
                .diff
                .interaction
                .controlled_selection
                == Some(true);
            if !controlled
                && (range.is_some()
                    || code_view
                        .state
                        .borrow()
                        .selected_lines
                        .as_ref()
                        .is_some_and(|selected| selected.id == *id))
            {
                code_view.apply_selected_lines(
                    selection.clone(),
                    SelectionWriteOptions {
                        notify: Some(false),
                        ..SelectionWriteOptions::default()
                    },
                );
            }
            let on_selected_lines_change = code_view
                .state
                .borrow()
                .options
                .on_selected_lines_change
                .clone();
            if let Some(on_selected_lines_change) = on_selected_lines_change {
                on_selected_lines_change(selection.as_ref());
            }
            let callback = select(&code_view.state.borrow().options.callbacks);
            if let Some(callback) = callback {
                callback(range, &context);
            }
        })
    }

    /// The item's cached wrapper in `slot`, made on first use.
    fn cached_selection_wrapper(
        &self,
        state: &Rc<ItemOptionsState>,
        slot: &mut Option<SelectionCallback>,
        select: SelectCallback<Rendered, LAnnotation>,
    ) -> SelectionCallback {
        Rc::clone(slot.get_or_insert_with(|| self.selection_wrapper(state, select)))
    }

    /// The `FileDiffOptions` of an item: CodeView's pass-through options, the
    /// mapped ones (`stickyHeader`, `hunkSeparators`, the item's `collapsed`)
    /// and the item's callback wrappers.
    pub(super) fn build_diff_options(
        &self,
        item: &CodeViewDiffItem<LAnnotation>,
        state: &Rc<ItemOptionsState>,
        cache: &mut ItemCallbackCache<Rendered, LAnnotation>,
    ) -> FileDiffOptions<NodeKey, Rendered, LAnnotation> {
        let current = self.state.borrow();
        let options = &current.options;
        let mut diff = options.diff.clone();
        diff.base.base.sticky_header = options.sticky_headers;
        diff.hunk_separators = options.hunk_separators.map(Into::into);
        diff.base.base.collapsed = item.collapsed;

        // A shared callback is wrapped only when CodeView has one, and the
        // wrapper is made on the first read.
        macro_rules! wrap_shared {
            ($field:ident, $cache:ident, $make:expr) => {
                if options.callbacks.$field.is_some() {
                    diff.$field = Some(cache.$cache.get_or_insert_with(|| $make).clone());
                }
            };
            ($group:ident . $field:ident, $cache:ident, $make:expr) => {
                if options.callbacks.$field.is_some() {
                    diff.$group.$field = Some(cache.$cache.get_or_insert_with(|| $make).clone());
                }
            };
        }
        wrap_shared!(
            render_custom_header,
            render_custom_header,
            PtrEq(Rc::new(shared_wrapper!(
                self,
                state,
                (file_diff: &FileDiffMetadata => file_diff) -> Option<Rendered>,
                render_custom_header
            )))
        );
        wrap_shared!(
            render_header_prefix,
            render_header_prefix,
            PtrEq(Rc::new(shared_wrapper!(
                self,
                state,
                (file_diff: &FileDiffMetadata => file_diff) -> Option<Rendered>,
                render_header_prefix
            )))
        );
        wrap_shared!(
            render_header_filename_suffix,
            render_header_filename_suffix,
            PtrEq(Rc::new(shared_wrapper!(
                self,
                state,
                (file_diff: &FileDiffMetadata => file_diff) -> Option<Rendered>,
                render_header_filename_suffix
            )))
        );
        wrap_shared!(
            render_header_metadata,
            render_header_metadata,
            PtrEq(Rc::new(shared_wrapper!(
                self,
                state,
                (file_diff: &FileDiffMetadata => file_diff) -> Option<Rendered>,
                render_header_metadata
            )))
        );
        wrap_shared!(
            render_annotation,
            render_annotation,
            PtrEq(Rc::new(shared_wrapper!(
                self,
                state,
                (annotation: &DiffLineAnnotation<LAnnotation> => annotation, file_diff: &FileDiffMetadata => file_diff) -> Option<Rendered>,
                render_annotation
            )))
        );
        wrap_shared!(
            render_gutter_utility,
            render_gutter_utility,
            PtrEq(Rc::new(shared_wrapper!(
                self,
                state,
                (get_hovered_line: &dyn Fn() -> Option<GetHoveredLineResult> => get_hovered_line) -> Option<Rendered>,
                render_gutter_utility
            )))
        );
        wrap_shared!(
            on_post_render,
            on_post_render,
            PtrEq(Rc::new(shared_wrapper!(
                self,
                state,
                (element: &NodeKey => element, phase: PostRenderPhase => &phase) -> (),
                on_post_render
            )))
        );
        wrap_shared!(
            interaction.on_gutter_utility_click,
            on_gutter_utility_click,
            Rc::new(shared_wrapper!(
                self,
                state,
                (range: SelectedLineRange => &range) -> (),
                on_gutter_utility_click
            ))
        );
        wrap_shared!(
            interaction.on_line_click,
            on_line_click,
            Rc::new(shared_wrapper!(
                self,
                state,
                (props: &LineEventProps<NodeKey> => props) -> (),
                on_line_click
            ))
        );
        wrap_shared!(
            interaction.on_line_number_click,
            on_line_number_click,
            Rc::new(shared_wrapper!(
                self,
                state,
                (props: &LineEventProps<NodeKey> => props) -> (),
                on_line_number_click
            ))
        );
        wrap_shared!(
            interaction.on_line_enter,
            on_line_enter,
            Rc::new(shared_wrapper!(
                self,
                state,
                (props: &LineEventProps<NodeKey> => props) -> (),
                on_line_enter
            ))
        );
        wrap_shared!(
            interaction.on_line_leave,
            on_line_leave,
            Rc::new(shared_wrapper!(
                self,
                state,
                (props: &LineEventProps<NodeKey> => props) -> (),
                on_line_leave
            ))
        );

        // The selection callbacks always call back into CodeView.
        diff.interaction.on_line_selected = Some(self.cached_selection_wrapper(
            state,
            &mut cache.on_line_selected,
            |callbacks| callbacks.on_line_selected.clone(),
        ));
        diff.interaction.on_line_selection_start = Some(self.cached_selection_wrapper(
            state,
            &mut cache.on_line_selection_start,
            |callbacks| callbacks.on_line_selection_start.clone(),
        ));
        diff.interaction.on_line_selection_change = Some(self.cached_selection_wrapper(
            state,
            &mut cache.on_line_selection_change,
            |callbacks| callbacks.on_line_selection_change.clone(),
        ));
        diff.interaction.on_line_selection_end = Some(self.cached_selection_wrapper(
            state,
            &mut cache.on_line_selection_end,
            |callbacks| callbacks.on_line_selection_end.clone(),
        ));
        diff
    }
}
