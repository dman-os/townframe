// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/FileDiff.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: PARTIAL port. S12a-1 (the options seam): `FileDiffOptions`, the
// theme and renderer-option derivations, the interaction wiring rule,
// `getLineIndexForDiff` and the option-dependent predicates of the class.
// S12a-2a (the struct and its first lifecycle seam): the class state that
// seam reads, the constructor wiring, `setOptions`, `syncInteractionOptions`,
// `setThemeType` (with `applyCachedThemeState` and `hasThemeChanged`), the
// annotation bookkeeping, selection deferral, `flushManagers`, `getLineIndex`
// and `handleExpandHunk`/`expandHunk`. Hydration, loading and `render` (2b),
// `rerender`, `cleanUp`, `isLineRenderable`, `getNearestRenderableLine` and
// `revealLine` (2c) and the DOM helpers (S12a-3) follow; see
// parity/closure_notes.md.
// * `FileDiffOptions<Element, Rendered, LAnnotation>`: `extends BaseDiffOptions,
//   InteractionManagerBaseOptions<'diff'>` is composition (`base`,
//   `interaction`). The callbacks upstream types as returning
//   `HTMLElement | string | number | null | undefined` return the host's
//   `Rendered` (a hast element, a Dioxus node...). Callbacks are `PtrEq`, so the
//   derived `PartialEq` compares them by identity as `areOptionsEqual` does.
//   `hunkSeparators` replaces `BaseDiffOptions.hunkSeparators`
//   (`Exclude<HunkSeparators, 'custom'> | function`); the function's second
//   parameter (the FileDiff instance) is not passed. `loadDiffFiles` is the flag
//   `has_diff_file_loader` (as in `DiffHunksRendererOptions`).
// * Dropped: `onEditChange`, `onEditComplete` and the `Caret` type parameter
//   (edit mode), the `workerManager` branch of `getTheme` (worker pool),
//   token callbacks (DEFER, see InteractionManager), `renderGutterUtility`'s
//   and `renderAnnotation`'s DOM return types (they return `Rendered`),
//   `onPostRender`'s `instance` argument.
// * `getLineIndexForDiff`: upstream's method takes the diff the caller picked
//   (`getDiffForLineIndex`); here it is a free function over
//   `Option<&FileDiffMetadata>`. Subtractions saturate at zero where
//   JavaScript's `Math.max(.., 0)` clamps.
// S12a-2a, the class:
// * `FileDiff` is an `Rc` (the interaction manager's `getLineIndex` and
//   `handleExpandHunk` callbacks reach back into the instance, as the arrow
//   functions upstream do) over `Cell`/`RefCell` state, with a `FileDiffHost`:
//   the four manager host traits (`VirtualizerHost` for `VirtualizedFileDiff`,
//   which extends this class) all answering the one `Document` the instance
//   renders into. The three managers and the renderer are built in the
//   constructor in upstream's order; the resize observer hub and the render
//   queue, which are module statics upstream, are constructor arguments shared
//   by `Rc` among the instances of a host. `workerManager` (worker pool) and
//   `requestOwnerRender` (arrives with `rerender`, its only reader) are not
//   constructor parameters; `__id`/`type` arrive with their first reader.
// * The editor and edit session are dropped, so the branches that read
//   `editSession` take the "no session" arm: `getWorkingDiff` is the input
//   diff, `getAnnotationSlotName` is `getLineAnnotationName` (a pure
//   function), `getLatestAnnotations` skips the session, and
//   `isNewAnnotations` is `!(lineAnnotations === latest)`. `setEditorActiveLine`
//   and `deferredEditorActiveLine` are dropped with the editor active line;
//   nothing else in the port sets `lineStateRefreshPending` (upstream sets it
//   only on edit-session paths), so the deferral arm of `setSelectedLines` is
//   reached only through `set_line_state_refresh_pending`, which exists so the
//   arm can be exercised and is the hook a later part uses.
// * Arrays with identity are `LineAnnotations` (`Rc<Vec<..>>`, compared with
//   `Rc::ptr_eq`) and the diffs are `Rc<FileDiffMetadata>` (`areDiffTargetsEqual`
//   compares addresses). `EMPTY_LINE_ANNOTATIONS` is one empty `Rc` per
//   instance, so another instance's empty array is not identical to it (not
//   observable by an owner that passes its own arrays).
// * `applyThemeState` (shadow-root CSS, S12a-3) is not ported: reaching it
//   panics with its arguments in the message. It is reachable only once
//   `appliedThemeCSS` and `fileContainer` are set, which `render` (2b) does.
// * `loadFilesIfNecessary` (2b) and `rerender` (2c), which `expandHunk` calls,
//   are `todo!()`; a hunk-expand click panics until they are ported.
// * `options` is mutated in place by `mergeOptions` (upstream replaces the
//   object by a spread copy); only the owner of the instance can tell.
// * Node ownership. The instance owns the nodes it creates in its `Document`.
//   The managers hold keys of nodes that have left the tree until their next
//   `setup` cleans them up (they write to and read from them), so a render
//   `retire`s the nodes it replaces (detached, still alive) and the instance
//   `discard`s them in `flush_managers` right after the managers' setup for
//   that render has run: the managers have then dropped every key. A
//   `flush_managers` that returns early (nothing dirty, or no `pre`) runs no
//   setup and discards nothing. `cleanUp` (2c) cleans the managers up first and
//   then discards what is still retired and the nodes it owns (the container
//   belongs to the caller and is never discarded). Retire subtree roots only:
//   a child retired after its parent was discarded panics as a stale key.

use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;

use crate::components::virtualizer::VirtualizerHost;
use crate::constants::default_themes;
use crate::document::NodeKey;
use crate::js_collections::PtrEq;
use crate::managers::interaction_manager::GetHoveredLineResult;
use crate::managers::interaction_manager::GetLineIndexUtility;
use crate::managers::interaction_manager::HunkExpandCallback;
use crate::managers::interaction_manager::InteractionHost;
use crate::managers::interaction_manager::InteractionManager;
use crate::managers::interaction_manager::InteractionManagerBaseOptions;
use crate::managers::interaction_manager::InteractionManagerMode;
use crate::managers::interaction_manager::InteractionManagerOptions;
use crate::managers::interaction_manager::InteractionPluckOptions;
use crate::managers::interaction_manager::SelectionWriteOptions;
use crate::managers::interaction_manager::pluck_interaction_options;
use crate::managers::resize_manager::ResizeManager;
use crate::managers::resize_manager::ResizeManagerColumnVariableMode;
use crate::managers::resize_manager::ResizeManagerHost;
use crate::managers::resize_manager::ResizeManagerSetupOptions;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::scroll_sync_manager::ScrollSyncHost;
use crate::managers::scroll_sync_manager::ScrollSyncManager;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::renderers::diff_hunks_renderer::DefaultHooks;
use crate::renderers::diff_hunks_renderer::DiffHunksRenderer;
use crate::types::lines::ExpansionDirections;
use crate::types::lines::HunkData;
use crate::types::options::AppliedThemeStyleCache;
use crate::types::options::BaseDiffOptions;
use crate::types::options::BaseThemeType;
use crate::types::options::DiffStyle;
use crate::types::options::HunkSeparators;
use crate::types::options::Overflow;
use crate::types::options::PostRenderPhase;
use crate::types::options::ThemeOption;
use crate::types::options::ThemeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectedLineRange;
use crate::types::view::SelectionSide;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_start_boundary;
use crate::utils::options::get_diff_hunks_renderer_options::get_diff_hunks_renderer_options;
use crate::utils::predicates::are_diff_targets_equal::are_diff_targets_equal;
use crate::utils::predicates::are_themes_equal::are_themes_equal;
use crate::utils::render::get_line_annotation_name::AnnotationName;
use crate::utils::render::get_line_annotation_name::get_line_annotation_name;

pub type RenderHunkSeparatorCallback<Rendered> = PtrEq<dyn Fn(&HunkData) -> Option<Rendered>>;

/// `Exclude<HunkSeparators, 'custom'> | ((hunk, instance) => ...)`
pub enum FileDiffHunkSeparators<Rendered> {
    Simple,
    Metadata,
    LineInfo,
    LineInfoBasic,
    /// Deprecated upstream.
    Custom(RenderHunkSeparatorCallback<Rendered>),
}

impl<Rendered> Clone for FileDiffHunkSeparators<Rendered> {
    fn clone(&self) -> Self {
        match self {
            Self::Simple => Self::Simple,
            Self::Metadata => Self::Metadata,
            Self::LineInfo => Self::LineInfo,
            Self::LineInfoBasic => Self::LineInfoBasic,
            Self::Custom(callback) => Self::Custom(callback.clone()),
        }
    }
}

impl<Rendered> PartialEq for FileDiffHunkSeparators<Rendered> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Simple, Self::Simple)
            | (Self::Metadata, Self::Metadata)
            | (Self::LineInfo, Self::LineInfo)
            | (Self::LineInfoBasic, Self::LineInfoBasic) => true,
            (Self::Custom(first), Self::Custom(second)) => first == second,
            _ => false,
        }
    }
}

pub type RenderHeaderCallback<Rendered> = PtrEq<dyn Fn(&FileDiffMetadata) -> Option<Rendered>>;
pub type RenderAnnotationCallback<Rendered, LAnnotation> =
    PtrEq<dyn Fn(&DiffLineAnnotation<LAnnotation>, &FileDiffMetadata) -> Option<Rendered>>;
pub type RenderGutterUtilityCallback<Rendered> =
    PtrEq<dyn Fn(&dyn Fn() -> Option<GetHoveredLineResult>) -> Option<Rendered>>;
pub type OnPostRenderCallback<Element> = PtrEq<dyn Fn(&Element, PostRenderPhase)>;

/// `FileDiffOptions<LAnnotation, Caret>`
pub struct FileDiffOptions<Element, Rendered, LAnnotation> {
    /// `Omit<BaseDiffOptions, 'hunkSeparators'>`: `base.hunk_separators` is
    /// unused, see `hunk_separators`.
    pub base: BaseDiffOptions,
    pub interaction: InteractionManagerBaseOptions<Element>,
    pub hunk_separators: Option<FileDiffHunkSeparators<Rendered>>,
    pub render_header_prefix: Option<RenderHeaderCallback<Rendered>>,
    pub render_header_filename_suffix: Option<RenderHeaderCallback<Rendered>>,
    pub render_header_metadata: Option<RenderHeaderCallback<Rendered>>,
    pub render_custom_header: Option<RenderHeaderCallback<Rendered>>,
    /// When true, errors during rendering are rethrown instead of being caught
    /// and displayed in the DOM.
    pub disable_error_handling: Option<bool>,
    pub render_annotation: Option<RenderAnnotationCallback<Rendered, LAnnotation>>,
    pub render_gutter_utility: Option<RenderGutterUtilityCallback<Rendered>>,
    pub on_post_render: Option<OnPostRenderCallback<Element>>,
    /// `loadDiffFiles != null`
    pub has_diff_file_loader: bool,
}

impl<Element, Rendered, LAnnotation> Default for FileDiffOptions<Element, Rendered, LAnnotation> {
    /// The constructor default `{ theme: DEFAULT_THEMES }`.
    fn default() -> Self {
        let mut base = BaseDiffOptions::default();
        base.base.theme = Some(ThemeOption::Themes(default_themes()));
        Self {
            base,
            interaction: InteractionManagerBaseOptions::default(),
            hunk_separators: None,
            render_header_prefix: None,
            render_header_filename_suffix: None,
            render_header_metadata: None,
            render_custom_header: None,
            disable_error_handling: None,
            render_annotation: None,
            render_gutter_utility: None,
            on_post_render: None,
            has_diff_file_loader: false,
        }
    }
}

impl<Element, Rendered, LAnnotation> Clone for FileDiffOptions<Element, Rendered, LAnnotation> {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            interaction: self.interaction.clone(),
            hunk_separators: self.hunk_separators.clone(),
            render_header_prefix: self.render_header_prefix.clone(),
            render_header_filename_suffix: self.render_header_filename_suffix.clone(),
            render_header_metadata: self.render_header_metadata.clone(),
            render_custom_header: self.render_custom_header.clone(),
            disable_error_handling: self.disable_error_handling,
            render_annotation: self.render_annotation.clone(),
            render_gutter_utility: self.render_gutter_utility.clone(),
            on_post_render: self.on_post_render.clone(),
            has_diff_file_loader: self.has_diff_file_loader,
        }
    }
}

impl<Element, Rendered, LAnnotation> PartialEq for FileDiffOptions<Element, Rendered, LAnnotation> {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base
            && self.interaction == other.interaction
            && self.hunk_separators == other.hunk_separators
            && self.render_header_prefix == other.render_header_prefix
            && self.render_header_filename_suffix == other.render_header_filename_suffix
            && self.render_header_metadata == other.render_header_metadata
            && self.render_custom_header == other.render_custom_header
            && self.disable_error_handling == other.disable_error_handling
            && self.render_annotation == other.render_annotation
            && self.render_gutter_utility == other.render_gutter_utility
            && self.on_post_render == other.on_post_render
            && self.has_diff_file_loader == other.has_diff_file_loader
    }
}

impl<Element, Rendered, LAnnotation> FileDiffOptions<Element, Rendered, LAnnotation> {
    /// `getTheme` without the worker pool: `options.theme ?? DEFAULT_THEMES`.
    pub fn get_theme(&self) -> ThemeOption {
        self.base
            .base
            .theme
            .clone()
            .unwrap_or_else(|| ThemeOption::Themes(default_themes()))
    }

    /// Whether `hunkSeparators` makes the separators expandable:
    /// `typeof hunkSeparators === 'function' || (hunkSeparators ?? 'line-info')
    /// === 'line-info' || hunkSeparators === 'line-info-basic'`, the condition
    /// under which the constructor and `syncInteractionOptions` hand
    /// `handleExpandHunk` to `pluckInteractionOptions`.
    pub fn expands_hunks_on_click(&self) -> bool {
        matches!(
            self.hunk_separators,
            None | Some(
                FileDiffHunkSeparators::Custom(_)
                    | FileDiffHunkSeparators::LineInfo
                    | FileDiffHunkSeparators::LineInfoBasic
            )
        )
    }

    /// `pluckInteractionOptions(this.options, expands ? this.handleExpandHunk :
    /// undefined, this.getLineIndex)`; the two callbacks belong to the instance.
    pub fn interaction_options(
        &self,
        handle_expand_hunk: Option<HunkExpandCallback>,
        get_line_index: Option<GetLineIndexUtility>,
    ) -> InteractionManagerOptions<Element> {
        pluck_interaction_options(
            InteractionPluckOptions {
                base: self.interaction.clone(),
                has_render_gutter_utility: self.render_gutter_utility.is_some(),
            },
            handle_expand_hunk.filter(|_| self.expands_hunks_on_click()),
            get_line_index,
        )
    }

    /// `canPartiallyRender`
    pub fn can_partially_render(
        &self,
        force_render: bool,
        annotations_changed: bool,
        did_content_change: bool,
    ) -> bool {
        !(force_render
            || annotations_changed
            || did_content_change
            || matches!(
                self.hunk_separators,
                Some(FileDiffHunkSeparators::Custom(_))
            ))
    }

    /// `shouldApplyColumnVariables`; `pre_has_merge_conflict` is
    /// `pre.hasAttribute('data-has-merge-conflict')` (merge conflicts are
    /// DEFER, so a host without them passes false).
    pub fn should_apply_column_variables(
        &self,
        overflow: crate::types::options::Overflow,
        line_annotation_count: usize,
        pre_has_merge_conflict: bool,
    ) -> bool {
        if matches!(
            self.hunk_separators,
            Some(FileDiffHunkSeparators::Custom(_))
        ) {
            return true;
        }
        overflow == crate::types::options::Overflow::Scroll
            && (line_annotation_count > 0 || pre_has_merge_conflict)
    }

    /// The `HunkSeparators` the renderer sees (`'custom'` for a function).
    pub fn renderer_hunk_separators(&self) -> Option<HunkSeparators> {
        self.hunk_separators
            .as_ref()
            .map(|separators| match separators {
                FileDiffHunkSeparators::Simple => HunkSeparators::Simple,
                FileDiffHunkSeparators::Metadata => HunkSeparators::Metadata,
                FileDiffHunkSeparators::LineInfo => HunkSeparators::LineInfo,
                FileDiffHunkSeparators::LineInfoBasic => HunkSeparators::LineInfoBasic,
                FileDiffHunkSeparators::Custom(_) => HunkSeparators::Custom,
            })
    }
}

// Resolve source lines against the same diff that produced the rendered
// rows. During an asynchronous replacement, that can be the previous diff.
pub fn get_line_index_for_diff(
    file_diff: Option<&FileDiffMetadata>,
    line_number: usize,
    side: SelectionSide,
) -> Option<(usize, usize)> {
    let file_diff = file_diff?;
    let last_hunk = file_diff.hunks.last();
    let mut target_unified_index: Option<usize> = None;
    let mut target_split_index: Option<usize> = None;
    for hunk in &file_diff.hunks {
        let (hunk_start, hunk_count) = if side == SelectionSide::Deletions {
            (hunk.deletion_start, hunk.deletion_count)
        } else {
            (hunk.addition_start, hunk.addition_count)
        };
        let mut current_line_number = get_hunk_side_start_boundary(hunk_start, hunk_count) + 1;
        let mut split_index = hunk.split_line_start;
        let mut unified_index = hunk.unified_line_start;

        // If we've selected a line between or before a hunk,
        // we should grab its index here
        if line_number < current_line_number {
            let difference = current_line_number - line_number;
            target_unified_index = Some(unified_index.saturating_sub(difference));
            target_split_index = Some(split_index.saturating_sub(difference));
            break;
        }

        if line_number >= current_line_number + hunk_count {
            if last_hunk.is_some_and(|last| std::ptr::eq(last, hunk)) {
                let difference = line_number - (current_line_number + hunk_count);
                target_unified_index = Some(unified_index + hunk.unified_line_count + difference);
                target_split_index = Some(split_index + hunk.split_line_count + difference);
                break;
            }
            continue;
        }

        for content in &hunk.hunk_content {
            match content {
                HunkContent::Context(context) => {
                    if line_number < current_line_number + context.lines {
                        let difference = line_number - current_line_number;
                        target_split_index = Some(split_index + difference);
                        target_unified_index = Some(unified_index + difference);
                        break;
                    }
                    current_line_number += context.lines;
                    split_index += context.lines;
                    unified_index += context.lines;
                }
                HunkContent::Change(change) => {
                    let side_count = if side == SelectionSide::Deletions {
                        change.deletions
                    } else {
                        change.additions
                    };
                    if line_number < current_line_number + side_count {
                        let index_difference = line_number - current_line_number;
                        target_unified_index = Some(
                            unified_index
                                + if side == SelectionSide::Additions {
                                    change.deletions
                                } else {
                                    0
                                }
                                + index_difference,
                        );
                        target_split_index = Some(split_index + index_difference);
                        break;
                    }
                    current_line_number += side_count;
                    split_index += change.deletions.max(change.additions);
                    unified_index += change.deletions + change.additions;
                }
            }
        }

        break;
    }

    Some((target_unified_index?, target_split_index?))
}

/// What `FileDiff` asks of its platform: the four manager host traits, all
/// answering the same `Document` the instance renders into (the trait system
/// cannot say so; an implementation whose `document()` methods disagree is a
/// bug). Implemented for every type that implements the four.
pub trait FileDiffHost:
    VirtualizerHost + ResizeManagerHost + ScrollSyncHost + InteractionHost
{
}

impl<Host> FileDiffHost for Host where
    Host: VirtualizerHost + ResizeManagerHost + ScrollSyncHost + InteractionHost
{
}

/// A `DiffLineAnnotation<LAnnotation>[]` with JavaScript's array identity.
pub type LineAnnotations<LAnnotation> = Rc<Vec<DiffLineAnnotation<LAnnotation>>>;

/// `FileDiffRenderData<LAnnotation>`
pub struct FileDiffRenderData<LAnnotation> {
    pub file_diff: Rc<FileDiffMetadata>,
    pub line_annotations: LineAnnotations<LAnnotation>,
}

impl<LAnnotation> Clone for FileDiffRenderData<LAnnotation> {
    fn clone(&self) -> Self {
        Self {
            file_diff: Rc::clone(&self.file_diff),
            line_annotations: Rc::clone(&self.line_annotations),
        }
    }
}

/// `HeaderCache`
#[derive(Default)]
pub struct HeaderCache {
    pub last_rendered_html: Option<String>,
    pub html: Option<String>,
    pub file_diff: Option<Rc<FileDiffMetadata>>,
}

/// `DeferredSelectedLinesWrite`
type DeferredSelectedLinesWrite = (Option<SelectedLineRange>, SelectionWriteOptions);

/// The fields of the class that change over its life. One `RefCell`; a borrow
/// is never held across a call into a manager, a host or a callback (they
/// re-enter the instance).
pub(crate) struct FileDiffState<LAnnotation> {
    pub(crate) file_container: Option<NodeKey>,
    pub(crate) pre: Option<NodeKey>,
    pub(crate) code_deletions: Option<NodeKey>,
    pub(crate) code_additions: Option<NodeKey>,
    pub(crate) applied_theme_css: Option<AppliedThemeStyleCache>,
    pub(crate) header_cache: HeaderCache,
    /// Latest input (`fileDiff`).
    pub(crate) file_diff: Option<Rc<FileDiffMetadata>>,
    /// Diff and annotations currently displayed.
    pub(crate) rendered_data: Option<FileDiffRenderData<LAnnotation>>,
    /// The next diff and its annotations, kept together until that diff renders.
    pub(crate) pending_data: Option<FileDiffRenderData<LAnnotation>>,
    pub(crate) managers_dirty: bool,
    pub(crate) line_state_refresh_pending: bool,
    pub(crate) deferred_selected_lines: Option<DeferredSelectedLinesWrite>,
    /// Nodes a render replaced: detached, discarded after the next manager
    /// setup (see the file header).
    pub(crate) retired: Vec<NodeKey>,
    /// `EMPTY_LINE_ANNOTATIONS`
    pub(crate) empty_line_annotations: LineAnnotations<LAnnotation>,
}

pub struct FileDiff<Host: FileDiffHost, Rendered: 'static, LAnnotation: 'static> {
    host: Rc<Host>,
    weak_self: Weak<Self>,
    options: RefCell<FileDiffOptions<NodeKey, Rendered, LAnnotation>>,
    hunks_renderer: RefCell<DiffHunksRenderer<LAnnotation>>,
    resize_manager: Rc<ResizeManager<Host>>,
    scroll_sync_manager: Rc<ScrollSyncManager<Host>>,
    interaction_manager: Rc<InteractionManager<Host>>,
    state: RefCell<FileDiffState<LAnnotation>>,
}

impl<Host: FileDiffHost, Rendered: 'static, LAnnotation: Clone + 'static>
    FileDiff<Host, Rendered, LAnnotation>
{
    /// `new FileDiff(options)`; pass `FileDiffOptions::default()` for upstream's
    /// `{ theme: DEFAULT_THEMES }` default.
    pub fn new(
        host: Rc<Host>,
        resize_observer_hub: Rc<ResizeObserverHub<Host>>,
        render_queue: UniversalRenderingManager<Host>,
        options: FileDiffOptions<NodeKey, Rendered, LAnnotation>,
    ) -> Rc<Self> {
        Rc::new_cyclic(|weak: &Weak<Self>| {
            // `createHunksRenderer`
            let hunks_renderer = DiffHunksRenderer::with_hooks(
                get_diff_hunks_renderer_options(&options),
                Box::new(Self::get_annotation_slot_name),
                Box::new(DefaultHooks),
            );
            let resize_manager = ResizeManager::new(Rc::clone(&host), resize_observer_hub);
            let scroll_sync_manager = ScrollSyncManager::new(Rc::clone(&host));
            let handle_expand_hunk: HunkExpandCallback = {
                let weak = Weak::clone(weak);
                Rc::new(
                    move |hunk_index, direction, expansion_line_count_override| {
                        weak.upgrade()
                            .expect("FileDiff outlives its interaction manager")
                            .handle_expand_hunk(
                                hunk_index,
                                direction,
                                expansion_line_count_override,
                            );
                    },
                )
            };
            let interaction_manager = InteractionManager::new(
                Rc::clone(&host),
                render_queue,
                InteractionManagerMode::Diff,
                options.interaction_options(
                    Some(handle_expand_hunk),
                    Some(Self::get_line_index_callback(weak)),
                ),
            );
            Self {
                host,
                weak_self: Weak::clone(weak),
                options: RefCell::new(options),
                hunks_renderer: RefCell::new(hunks_renderer),
                resize_manager,
                scroll_sync_manager,
                interaction_manager,
                state: RefCell::new(FileDiffState {
                    file_container: None,
                    pre: None,
                    code_deletions: None,
                    code_additions: None,
                    applied_theme_css: None,
                    header_cache: HeaderCache::default(),
                    file_diff: None,
                    rendered_data: None,
                    pending_data: None,
                    managers_dirty: false,
                    line_state_refresh_pending: false,
                    deferred_selected_lines: None,
                    retired: Vec::new(),
                    empty_line_annotations: Rc::new(Vec::new()),
                }),
            }
        })
    }

    // `this.getLineIndex`, the arrow function the interaction manager holds.
    fn get_line_index_callback(weak: &Weak<Self>) -> GetLineIndexUtility {
        let weak = Weak::clone(weak);
        Rc::new(move |line_number, side| {
            weak.upgrade()
                .expect("FileDiff outlives its interaction manager")
                .get_line_index(line_number, side)
        })
    }

    // `FileDiff` reaches its document through the interaction host; the four
    // host traits answer the same one.
    fn document(&self) -> &crate::document::Document {
        InteractionHost::document(&*self.host)
    }

    pub fn options(&self) -> std::cell::Ref<'_, FileDiffOptions<NodeKey, Rendered, LAnnotation>> {
        self.options.borrow()
    }

    pub fn hunks_renderer(&self) -> std::cell::Ref<'_, DiffHunksRenderer<LAnnotation>> {
        self.hunks_renderer.borrow()
    }

    pub fn resize_manager(&self) -> &Rc<ResizeManager<Host>> {
        &self.resize_manager
    }

    pub fn scroll_sync_manager(&self) -> &Rc<ScrollSyncManager<Host>> {
        &self.scroll_sync_manager
    }

    pub fn interaction_manager(&self) -> &Rc<InteractionManager<Host>> {
        &self.interaction_manager
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> std::cell::RefMut<'_, FileDiffState<LAnnotation>> {
        self.state.borrow_mut()
    }

    /// `getTheme` without the worker pool.
    pub fn get_theme(&self) -> ThemeOption {
        self.options.borrow().get_theme()
    }

    /// `getAnnotationSlotName` without an edit session.
    pub fn get_annotation_slot_name(annotation: &DiffLineAnnotation<LAnnotation>) -> String {
        get_line_annotation_name(AnnotationName::DiffLine {
            side: annotation.side,
            line_number: annotation.line_number,
        })
    }

    /// `getLineIndex`
    pub fn get_line_index(
        &self,
        line_number: usize,
        side: Option<SelectionSide>,
    ) -> Option<(i64, i64)> {
        let side = side.unwrap_or(SelectionSide::Additions);
        let diff = self.get_diff_for_line_index();
        get_line_index_for_diff(diff.as_deref(), line_number, side).map(|(unified, split)| {
            (
                i64::try_from(unified).expect("line index fits i64"),
                i64::try_from(split).expect("line index fits i64"),
            )
        })
    }

    pub fn get_diff_for_line_index(&self) -> Option<Rc<FileDiffMetadata>> {
        self.get_rendered_diff()
    }

    // FIXME(amadeus): This is a bit of a looming issue that I'll need to resolve:
    // * Do we publicly allow merging of options or do we have individualized setters?
    // * When setting new options, we need to figure out what settings require a
    //   re-render and which can just be applied more elegantly
    // * There's also an issue of options that live here on the File class and
    //   those that live on the Hunk class, and it's a bit of an issue with passing
    //   settings down and mirroring them (not great...)
    pub fn set_options(&self, options: Option<FileDiffOptions<NodeKey, Rendered, LAnnotation>>) {
        let Some(options) = options else { return };
        let renderer_options = get_diff_hunks_renderer_options(&options);
        *self.options.borrow_mut() = options;
        self.clear_reusable_header();
        self.hunks_renderer
            .borrow_mut()
            .set_options(renderer_options);
        self.sync_interaction_options();
    }

    pub fn sync_interaction_options(&self) {
        let handle_expand_hunk: HunkExpandCallback = {
            let weak = Weak::clone(&self.weak_self);
            Rc::new(
                move |hunk_index, direction, expansion_line_count_override| {
                    weak.upgrade()
                        .expect("FileDiff outlives its interaction manager")
                        .handle_expand_hunk(hunk_index, direction, expansion_line_count_override);
                },
            )
        };
        let interaction_options = self.options.borrow().interaction_options(
            Some(handle_expand_hunk),
            Some(Self::get_line_index_callback(&self.weak_self)),
        );
        self.interaction_manager.set_options(interaction_options);
    }

    pub fn set_theme_type(&self, theme_type: ThemeTypes) {
        let current = self
            .options
            .borrow()
            .base
            .base
            .theme_type
            .unwrap_or(ThemeTypes::System);
        if current == theme_type {
            return;
        }
        // `mergeOptions({ themeType })`
        self.options.borrow_mut().base.base.theme_type = Some(theme_type);
        self.apply_cached_theme_state(theme_type);
    }

    pub fn apply_cached_theme_state(&self, theme_type: ThemeTypes) -> bool {
        let (file_container, applied_theme_css) = {
            let state = self.state.borrow();
            (state.file_container, state.applied_theme_css.clone())
        };
        let theme_is_name = matches!(
            self.options.borrow().base.base.theme,
            Some(ThemeOption::Name(_))
        );
        let (false, Some(file_container), Some(applied_theme_css)) =
            (theme_is_name, file_container, applied_theme_css)
        else {
            return false;
        };
        let effective_theme_type = match applied_theme_css.base_theme_type {
            Some(BaseThemeType::Light) => ThemeTypes::Light,
            Some(BaseThemeType::Dark) => ThemeTypes::Dark,
            None => theme_type,
        };
        if applied_theme_css.theme_type == effective_theme_type {
            return false;
        }
        self.apply_theme_state(
            file_container,
            &applied_theme_css.theme_styles,
            theme_type,
            applied_theme_css.base_theme_type,
        );
        true
    }

    /// `applyThemeState` (shadow-root theme CSS) belongs to S12a-3.
    fn apply_theme_state(
        &self,
        container: NodeKey,
        theme_styles: &str,
        theme_type: ThemeTypes,
        base_theme_type: Option<BaseThemeType>,
    ) {
        todo!(
            "S12a-3 applyThemeState({container:?}, {theme_styles:?}, {theme_type:?}, \
             {base_theme_type:?})"
        )
    }

    pub fn has_theme_changed(&self) -> bool {
        let theme = self.get_theme();
        self.state
            .borrow()
            .applied_theme_css
            .as_ref()
            .is_some_and(|applied| !are_themes_equal(Some(&applied.theme), Some(&theme)))
    }

    pub fn get_hovered_line(&self) -> Option<GetHoveredLineResult> {
        self.interaction_manager.get_hovered_line()
    }

    /// The annotations of the displayed diff (`lineAnnotations`).
    pub fn line_annotations(&self) -> LineAnnotations<LAnnotation> {
        let state = self.state.borrow();
        match &state.rendered_data {
            Some(rendered) => Rc::clone(&rendered.line_annotations),
            None => Rc::clone(&state.empty_line_annotations),
        }
    }

    // Return annotations for the working diff, including updates that haven't
    // been rendered yet.
    pub fn get_latest_annotations(&self) -> LineAnnotations<LAnnotation> {
        let pending = self
            .state
            .borrow()
            .pending_data
            .as_ref()
            .map(|pending| Rc::clone(&pending.line_annotations));
        pending.unwrap_or_else(|| self.line_annotations())
    }

    // Use pending annotations only when their diff is selected for rendering.
    pub fn get_annotations_for_diff(
        &self,
        file_diff: &FileDiffMetadata,
    ) -> LineAnnotations<LAnnotation> {
        let pending = self
            .state
            .borrow()
            .pending_data
            .as_ref()
            .filter(|pending| are_diff_targets_equal(Some(file_diff), Some(&*pending.file_diff)))
            .map(|pending| Rc::clone(&pending.line_annotations));
        pending.unwrap_or_else(|| self.line_annotations())
    }

    // Check whether the annotation array is new.
    pub fn is_new_annotations(&self, line_annotations: &LineAnnotations<LAnnotation>) -> bool {
        let latest_annotations = self.get_latest_annotations();
        if Rc::ptr_eq(line_annotations, &latest_annotations) {
            return false;
        }
        // No edit session: `session == null`.
        true
    }

    pub fn set_line_annotations(&self, line_annotations: LineAnnotations<LAnnotation>) {
        let file_diff = self.get_working_diff(None);
        if let Some(file_diff) = file_diff {
            self.state.borrow_mut().pending_data = Some(FileDiffRenderData {
                file_diff,
                line_annotations,
            });
        }
    }

    /// The latest input without editor changes.
    pub fn get_input_diff(&self) -> Option<Rc<FileDiffMetadata>> {
        self.state.borrow().file_diff.clone()
    }

    /// The input, or `fallback_diff` if the new input is not stored yet; without
    /// an edit session that is all `getWorkingDiff` returns.
    pub fn get_working_diff(
        &self,
        fallback_diff: Option<Rc<FileDiffMetadata>>,
    ) -> Option<Rc<FileDiffMetadata>> {
        fallback_diff.or_else(|| self.state.borrow().file_diff.clone())
    }

    /// The displayed diff.
    pub fn get_rendered_diff(&self) -> Option<Rc<FileDiffMetadata>> {
        self.state
            .borrow()
            .rendered_data
            .as_ref()
            .map(|rendered| Rc::clone(&rendered.file_diff))
    }

    pub fn clear_reusable_header(&self) {
        let mut state = self.state.borrow_mut();
        state.header_cache.html = None;
        state.header_cache.file_diff = None;
    }

    pub fn set_selected_lines(
        &self,
        range: Option<SelectedLineRange>,
        options: SelectionWriteOptions,
    ) {
        let deferred = {
            let mut state = self.state.borrow_mut();
            if state.line_state_refresh_pending {
                state.deferred_selected_lines = Some((range.clone(), options));
                true
            } else {
                false
            }
        };
        if !deferred {
            self.interaction_manager.set_selection(range, options);
        }
    }

    /// Whether a refresh is rebuilding the rows, so selected-line writes wait
    /// (`lineStateRefreshPending = ...`).
    pub fn set_line_state_refresh_pending(&self, pending: bool) {
        self.state.borrow_mut().line_state_refresh_pending = pending;
    }

    // Apply the latest selected-lines write after the rows are stable.
    pub fn flush_deferred_line_state(&self) {
        let selected_lines = {
            let mut state = self.state.borrow_mut();
            state.line_state_refresh_pending = false;
            state.deferred_selected_lines.take()
        };
        if let Some((range, options)) = selected_lines {
            self.interaction_manager.set_selection(range, options);
        }
    }

    pub fn flush_managers(&self) {
        let (managers_dirty, pre, code_deletions, code_additions) = {
            let state = self.state.borrow();
            (
                state.managers_dirty,
                state.pre,
                state.code_deletions,
                state.code_additions,
            )
        };
        let Some(pre) = pre.filter(|_| managers_dirty) else {
            self.state.borrow_mut().managers_dirty = false;
            return;
        };

        let (diff_style, overflow) = {
            let options = self.options.borrow();
            (
                options.base.diff_style.unwrap_or(DiffStyle::Split),
                options.base.base.overflow.unwrap_or(Overflow::Scroll),
            )
        };
        self.interaction_manager.setup(&pre);
        self.resize_manager.setup(
            &pre,
            ResizeManagerSetupOptions {
                disable_annotations: overflow == Overflow::Wrap,
                column_variables: if self.should_apply_column_variables(overflow) {
                    ResizeManagerColumnVariableMode::Apply
                } else {
                    ResizeManagerColumnVariableMode::Measure
                },
            },
        );
        if overflow == Overflow::Scroll && diff_style == DiffStyle::Split {
            self.scroll_sync_manager
                .setup(&pre, code_deletions, code_additions);
        } else {
            self.scroll_sync_manager.clean_up();
        }
        // Every manager has set up against the current rows and dropped the
        // keys of the nodes this render replaced.
        let retired = {
            let mut state = self.state.borrow_mut();
            state.managers_dirty = false;
            std::mem::take(&mut state.retired)
        };
        for node in retired {
            self.document().discard(node);
        }
    }

    /// A node a render replaced, detached from the tree and now the instance's
    /// to discard once the managers have set up (see the file header).
    pub fn retire(&self, node: NodeKey) {
        self.document().remove(node);
        self.state.borrow_mut().retired.push(node);
    }

    pub fn should_apply_column_variables(&self, overflow: Overflow) -> bool {
        let line_annotation_count = self.line_annotations().len();
        let pre = self.state.borrow().pre;
        let pre_has_merge_conflict = pre.is_some_and(|pre| {
            self.document()
                .get_attribute(pre, "data-has-merge-conflict")
                .is_some()
        });
        self.options.borrow().should_apply_column_variables(
            overflow,
            line_annotation_count,
            pre_has_merge_conflict,
        )
    }

    // This wrapper must stay separate from `expandHunk` because subclasses like
    // `VirtualizedFileDiff` replace `expandHunk` with their own instance field
    // after `super()` returns. `InteractionManager` is created in this base
    // constructor, so it needs a stable callback that resolves `this.expandHunk`
    // at click time instead of capturing the base implementation too early.
    pub fn handle_expand_hunk(
        &self,
        hunk_index: i64,
        direction: ExpansionDirections,
        expansion_line_count_override: Option<f64>,
    ) {
        self.expand_hunk(hunk_index, direction, expansion_line_count_override);
    }

    pub fn expand_hunk(
        &self,
        hunk_index: i64,
        direction: ExpansionDirections,
        expansion_line_count_override: Option<f64>,
    ) {
        self.load_files_if_necessary();
        self.hunks_renderer.borrow_mut().expand_hunk(
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
        self.rerender();
    }

    /// `loadFilesIfNecessary` is S12a-2b.
    fn load_files_if_necessary(&self) {
        todo!("S12a-2b loadFilesIfNecessary")
    }

    /// `rerender` is S12a-2c.
    fn rerender(&self) {
        todo!("S12a-2c rerender")
    }
}
