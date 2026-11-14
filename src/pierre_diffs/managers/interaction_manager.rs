// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/managers/InteractionManager.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: partial port (line hover, line and gutter selection, the selection
// renderer, the gutter utility, hunk expansion clicks):
// * DOM access goes through `InteractionHost` over an opaque `Element` key. The
//   guest's rendered hast model answers structural reads (parent, children,
//   attributes, sibling, the attribute-keyed subtree query) synchronously;
//   attribute writes and the gutter utility nodes are edits of that model.
//   Nothing else of the DOM is read. `event.composedPath()` is a model walk
//   from the event's target element to the root (`get_element_path`), and an
//   event arrives as `PointerEventData` (the target as a node key plus the
//   fields upstream reads: pointer id and type, button, shift key, client
//   coordinates). `preventDefault`/`stopPropagation` are the returned
//   `PointerResponse`. Coordinates are used only where upstream uses
//   `elementFromPoint` (a drag), behind `InteractionHost::element_from_point`.
// * Listener wiring is the host's: `syncPointerListeners` asks the host to
//   (un)register the four `pre` listeners and the three document listeners,
//   and the host calls the public `handle_*` methods (the arrow-function
//   handlers upstream registers itself). The class is an `Rc` so the render
//   callback can hold a weak reference, as the arrow function holds `this`;
//   state is `Cell`/`RefCell` behind `&self` because callbacks re-enter.
// * `TMode` is the runtime `InteractionManagerMode` (upstream uses it as a type
//   parameter and as `this.mode`); `EventBaseProps<TMode>` is
//   `LineEventBaseProps` with the diff-only fields optional.
// * Callbacks are `Rc<dyn Fn(..)>` and take the event data by reference.
// * The class is split into the manager and three views over it in
//   `interaction_manager/` (`Targets`: path and target resolution;
//   `SelectionRows`: the row-range writes of `renderSelection`; `GutterUtility`:
//   the utility node and its placement), only to keep each type inside the
//   guardrails method budget; the methods keep upstream's names and bodies.
// * `renderSelection` is queued through the injected `UniversalRenderingManager`.
// * `lineNumber`s are `usize` through `SelectionPoint`/`SelectedLineRange`
//   (types.ts as ported); a `data-line` below zero is therefore not
//   selectable (upstream would select it).
// Dropped (see parity/dropped.psv): token interactions (`onToken*`,
// `hoveredToken`, `TokenCache`, `enableTokenInteractionsOnWhitespace`: the
// `data-char` skip in `resolvePointerTarget` is kept because it decides which
// attributes a node contributes to the line target), merge-conflict actions
// (`onMergeConflictActionClick`, `MergeConflictActionTarget`, the
// `data-merge-conflict-actions` selection tweak), the editor active line
// (`setEditorActiveLine` and its render channel; its dirty flag survives as
// `rendered_editor_clean` because `isSelectionDirty` reads it), and
// `__debugPointerEvents` logging.

mod gutter_utility;
mod selection_rows;
mod targets;

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;

use self::gutter_utility::GutterUtility;
use self::selection_rows::SelectionRows;
use self::targets::Targets;
use crate::document::Document;
use crate::document::NodeKey;
use crate::js_text::parse_int;
use crate::managers::universal_rendering_manager::AnimationFrames;
use crate::managers::universal_rendering_manager::RenderCallback;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::lines::ExpansionDirections;
use crate::types::lines::LineTypes;
use crate::types::view::AnnotationSide;
use crate::types::view::SelectedLineRange;
use crate::types::view::SelectionPoint;
use crate::types::view::SelectionSide;
use crate::utils::dom::is_gutter_utility_path::is_gutter_utility_path;
use crate::utils::predicates::are_selection_points_equal::are_selection_points_equal;
use crate::utils::predicates::are_selections_equal::are_selections_equal;
use crate::utils::render::create_gutter_utility_element::create_gutter_utility_element;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionManagerMode {
    File,
    Diff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LineHoverHighlight {
    #[default]
    Disabled,
    Both,
    Number,
    Line,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointerType {
    Mouse,
    Touch,
    Pen,
}

/// The parts of a `PointerEvent`/`MouseEvent` the manager reads. `target` is
/// the element the event was dispatched at, as a key of the rendered model.
#[derive(Debug, Clone, PartialEq)]
pub struct PointerEventData<Element> {
    pub pointer_id: i32,
    pub pointer_type: PointerType,
    pub button: i16,
    pub shift_key: bool,
    pub client_x: f64,
    pub client_y: f64,
    pub target: Option<Element>,
}

/// What upstream does to the event after handling it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PointerResponse {
    pub prevent_default: bool,
    pub stop_propagation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineEventKind {
    Line,
    DiffLine,
}

/// `LineEventBaseProps | DiffLineEventBaseProps`: `annotation_side` and
/// `line_type` are the diff-only fields.
#[derive(Debug, Clone, PartialEq)]
pub struct LineEventBaseProps<Element> {
    pub kind: LineEventKind,
    pub line_element: Element,
    pub line_number: i64,
    pub number_column: bool,
    pub number_element: Element,
    pub annotation_side: Option<AnnotationSide>,
    pub line_type: Option<LineTypes>,
}

/// `OnLineClickProps` / `OnLineEnterLeaveProps` and the diff variants.
#[derive(Debug, Clone, PartialEq)]
pub struct LineEventProps<Element> {
    pub base: LineEventBaseProps<Element>,
    pub event: PointerEventData<Element>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetHoveredLineResult {
    pub line_number: i64,
    pub side: Option<AnnotationSide>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SelectionWriteOptions {
    pub notify: Option<bool>,
    // Limit the selected-line highlight to one side's column of a split diff.
    // It has no effect on unified or single-file views, whose one code column
    // carries no side attribute.
    pub active_line_side: Option<SelectionSide>,
    // Highlight only the gutter line number, not the line background.
    pub line_number_only: bool,
}

pub type GetLineIndexUtility = Rc<dyn Fn(usize, Option<SelectionSide>) -> Option<(i64, i64)>>;
pub type LineCallback<Element> = Rc<dyn Fn(&LineEventProps<Element>)>;
pub type SelectionCallback = Rc<dyn Fn(Option<&SelectedLineRange>)>;
pub type HunkExpandCallback = Rc<dyn Fn(i64, ExpansionDirections, Option<f64>)>;

pub struct InteractionManagerBaseOptions<Element> {
    pub line_hover_highlight: Option<LineHoverHighlight>,
    pub enable_gutter_utility: Option<bool>,
    pub on_gutter_utility_click: Option<Rc<dyn Fn(SelectedLineRange)>>,
    pub on_line_click: Option<LineCallback<Element>>,
    pub on_line_number_click: Option<LineCallback<Element>>,
    pub on_line_enter: Option<LineCallback<Element>>,
    pub on_line_leave: Option<LineCallback<Element>>,
    pub enable_line_selection: Option<bool>,
    pub controlled_selection: Option<bool>,
    pub on_line_selected: Option<SelectionCallback>,
    pub on_line_selection_start: Option<SelectionCallback>,
    pub on_line_selection_change: Option<SelectionCallback>,
    pub on_line_selection_end: Option<SelectionCallback>,
    pub get_line_index: Option<GetLineIndexUtility>,
}

// Callbacks are compared by identity (`===`), as `areOptionsEqual` compares them.
fn rc_eq<Callback: ?Sized>(first: &Option<Rc<Callback>>, second: &Option<Rc<Callback>>) -> bool {
    match (first, second) {
        (Some(first), Some(second)) => std::ptr::addr_eq(Rc::as_ptr(first), Rc::as_ptr(second)),
        (None, None) => true,
        _ => false,
    }
}

impl<Element> Clone for InteractionManagerBaseOptions<Element> {
    fn clone(&self) -> Self {
        Self {
            line_hover_highlight: self.line_hover_highlight,
            enable_gutter_utility: self.enable_gutter_utility,
            on_gutter_utility_click: self.on_gutter_utility_click.clone(),
            on_line_click: self.on_line_click.clone(),
            on_line_number_click: self.on_line_number_click.clone(),
            on_line_enter: self.on_line_enter.clone(),
            on_line_leave: self.on_line_leave.clone(),
            enable_line_selection: self.enable_line_selection,
            controlled_selection: self.controlled_selection,
            on_line_selected: self.on_line_selected.clone(),
            on_line_selection_start: self.on_line_selection_start.clone(),
            on_line_selection_change: self.on_line_selection_change.clone(),
            on_line_selection_end: self.on_line_selection_end.clone(),
            get_line_index: self.get_line_index.clone(),
        }
    }
}

impl<Element> PartialEq for InteractionManagerBaseOptions<Element> {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            line_hover_highlight,
            enable_gutter_utility,
            on_gutter_utility_click,
            on_line_click,
            on_line_number_click,
            on_line_enter,
            on_line_leave,
            enable_line_selection,
            controlled_selection,
            on_line_selected,
            on_line_selection_start,
            on_line_selection_change,
            on_line_selection_end,
            get_line_index,
        } = self;
        *line_hover_highlight == other.line_hover_highlight
            && *enable_gutter_utility == other.enable_gutter_utility
            && rc_eq(on_gutter_utility_click, &other.on_gutter_utility_click)
            && rc_eq(on_line_click, &other.on_line_click)
            && rc_eq(on_line_number_click, &other.on_line_number_click)
            && rc_eq(on_line_enter, &other.on_line_enter)
            && rc_eq(on_line_leave, &other.on_line_leave)
            && *enable_line_selection == other.enable_line_selection
            && *controlled_selection == other.controlled_selection
            && rc_eq(on_line_selected, &other.on_line_selected)
            && rc_eq(on_line_selection_start, &other.on_line_selection_start)
            && rc_eq(on_line_selection_change, &other.on_line_selection_change)
            && rc_eq(on_line_selection_end, &other.on_line_selection_end)
            && rc_eq(get_line_index, &other.get_line_index)
    }
}

impl<Element> Default for InteractionManagerBaseOptions<Element> {
    fn default() -> Self {
        Self {
            line_hover_highlight: None,
            enable_gutter_utility: None,
            on_gutter_utility_click: None,
            on_line_click: None,
            on_line_number_click: None,
            on_line_enter: None,
            on_line_leave: None,
            enable_line_selection: None,
            controlled_selection: None,
            on_line_selected: None,
            on_line_selection_start: None,
            on_line_selection_change: None,
            on_line_selection_end: None,
            get_line_index: None,
        }
    }
}

pub struct InteractionManagerOptions<Element> {
    pub base: InteractionManagerBaseOptions<Element>,
    pub uses_custom_gutter_utility: Option<bool>,
    pub on_hunk_expand: Option<HunkExpandCallback>,
}

impl<Element> Default for InteractionManagerOptions<Element> {
    fn default() -> Self {
        Self {
            base: InteractionManagerBaseOptions::default(),
            uses_custom_gutter_utility: None,
            on_hunk_expand: None,
        }
    }
}

/// `InteractionPluckOptions`: the base options plus whether `renderGutterUtility`
/// was given (the render callback itself belongs to the consuming component).
pub struct InteractionPluckOptions<Element> {
    pub base: InteractionManagerBaseOptions<Element>,
    pub has_render_gutter_utility: bool,
}

pub fn pluck_interaction_options<Element>(
    options: InteractionPluckOptions<Element>,
    on_hunk_expand: Option<HunkExpandCallback>,
    get_line_index: Option<GetLineIndexUtility>,
) -> InteractionManagerOptions<Element> {
    let InteractionPluckOptions {
        mut base,
        has_render_gutter_utility,
    } = options;
    let enable_gutter_utility = resolve_enable_gutter_utility_option(
        base.enable_gutter_utility,
        has_render_gutter_utility,
        base.on_gutter_utility_click.is_some(),
    );
    base.enable_gutter_utility = Some(enable_gutter_utility);
    base.get_line_index = get_line_index;
    InteractionManagerOptions {
        base,
        uses_custom_gutter_utility: Some(has_render_gutter_utility),
        on_hunk_expand,
    }
}

fn resolve_enable_gutter_utility_option(
    enable_gutter_utility: Option<bool>,
    has_render_gutter_utility: bool,
    has_on_gutter_utility_click: bool,
) -> bool {
    assert!(
        !(has_on_gutter_utility_click && has_render_gutter_utility),
        "Cannot use both 'onGutterUtilityClick' and 'renderGutterUtility'. Use only one gutter utility API."
    );
    enable_gutter_utility.unwrap_or(false)
}

/// The result of `elementFromPoint`: `Unavailable` is upstream's `undefined`
/// (hit-testing cannot run, callers fall back to the event path), `NoElement`
/// its `null` (it ran and found nothing).
#[derive(Debug, Clone, PartialEq)]
pub enum HitTest<Element> {
    Unavailable,
    NoElement,
    Element(Element),
}

/// What the platform supplies: the rendered document, hit-testing and event
/// registration. The structural reads and writes are the `Document`'s.
pub trait InteractionHost: AnimationFrames + Sized + 'static {
    fn document(&self) -> &Document;
    fn element_from_point(&self, x: f64, y: f64) -> HitTest<NodeKey>;
    /// `click`, `pointerdown`, `pointermove`, `pointerleave` on the `pre`.
    fn add_pre_listeners(&self, pre: NodeKey);
    fn remove_pre_listeners(&self, pre: NodeKey);
    /// `pointermove`, `pointerup`, `pointercancel` on the document.
    fn add_document_listeners(&self);
    fn remove_document_listeners(&self);
}

struct TargetFields<Element> {
    line_type: LineTypes,
    line_element: Element,
    line_number: i64,
    number_column: bool,
    number_element: Element,
    side: Option<AnnotationSide>,
    split_line_index: Option<i64>,
}

enum ResolvedPointerTarget<Element> {
    Line(TargetFields<Element>),
    /// `ExpandoEventProps`
    Expando {
        hunk_index: i64,
        direction: ExpansionDirections,
        all: bool,
    },
}

struct SelectionInfo {
    line_number: usize,
    event_side: Option<SelectionSide>,
    line_index: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectionHitSource {
    EventPath,
    CoordinatesFirst,
}

#[derive(Debug, Clone, Copy)]
struct SelectionHitOptions {
    source: SelectionHitSource,
    exclude_utility: Option<bool>,
}

struct SelectionEnds {
    top: SelectionPoint,
    bottom: SelectionPoint,
}

#[derive(Debug, Clone, PartialEq)]
struct LineRenderState {
    range: Option<SelectedLineRange>,
    highlight_side: Option<SelectionSide>,
    line_number_only: bool,
}

struct LineRenderColumn<Element> {
    content: Element,
    gutter: Element,
}

#[derive(Debug, Clone, PartialEq)]
enum PointerSession {
    Idle,
    Selecting {
        pointer_id: i32,
    },
    PendingSingleLineUnselect {
        pointer_id: i32,
        anchor: SelectionPoint,
        pending: SelectionPoint,
    },
    GutterSelecting {
        pointer_id: i32,
        anchor: SelectionPoint,
        current: SelectionPoint,
    },
}

/// `pathFromCoordinates`: `undefined` / `null` / a path.
enum CoordinatePath<Element> {
    Unavailable,
    NoElement,
    Path(Vec<Element>),
}

pub struct InteractionManager<Host: InteractionHost> {
    host: Rc<Host>,
    render_queue: UniversalRenderingManager<Host>,
    render_selection_callback: RenderCallback,
    mode: InteractionManagerMode,
    options: RefCell<Rc<InteractionManagerOptions<NodeKey>>>,

    hovered_line: RefCell<Option<LineEventBaseProps<NodeKey>>>,
    pre: RefCell<Option<NodeKey>>,

    gutter_utility_line: RefCell<Option<LineEventBaseProps<NodeKey>>>,
    gutter_utility_container: RefCell<Option<NodeKey>>,
    gutter_utility_button: RefCell<Option<NodeKey>>,
    gutter_utility_slot: RefCell<Option<NodeKey>>,

    interactive_lines_attr: Cell<bool>,
    interactive_line_numbers_attr: Cell<bool>,

    has_pointer_listeners: Cell<bool>,
    has_document_pointer_listeners: Cell<bool>,

    selected_range: RefCell<Option<SelectedLineRange>>,
    selected_range_highlight_side: Cell<Option<SelectionSide>>,
    selected_range_line_number_only: Cell<bool>,
    proposed_selected_range: RefCell<Option<Option<SelectedLineRange>>>,
    rendered_selected_lines_state: RefCell<Option<LineRenderState>>,
    // The editor active line's render channel is dropped; its "clean" flag is
    // what `isSelectionDirty` and the early return in `renderSelection` read.
    rendered_editor_clean: Cell<bool>,
    selection_anchor: Cell<Option<SelectionPoint>>,
    pointer_session: RefCell<PointerSession>,
}

impl<Host: InteractionHost> InteractionManager<Host> {
    pub fn new(
        host: Rc<Host>,
        render_queue: UniversalRenderingManager<Host>,
        mode: InteractionManagerMode,
        options: InteractionManagerOptions<NodeKey>,
    ) -> Rc<Self> {
        Rc::new_cyclic(|weak: &Weak<Self>| {
            let weak = Weak::clone(weak);
            let render_selection_callback: RenderCallback = Rc::new(move |_time| {
                if let Some(manager) = weak.upgrade() {
                    manager.render_selection();
                }
            });
            Self {
                host,
                render_queue,
                render_selection_callback,
                mode,
                options: RefCell::new(Rc::new(options)),
                hovered_line: RefCell::new(None),
                pre: RefCell::new(None),
                gutter_utility_line: RefCell::new(None),
                gutter_utility_container: RefCell::new(None),
                gutter_utility_button: RefCell::new(None),
                gutter_utility_slot: RefCell::new(None),
                interactive_lines_attr: Cell::new(false),
                interactive_line_numbers_attr: Cell::new(false),
                has_pointer_listeners: Cell::new(false),
                has_document_pointer_listeners: Cell::new(false),
                selected_range: RefCell::new(None),
                selected_range_highlight_side: Cell::new(None),
                selected_range_line_number_only: Cell::new(false),
                proposed_selected_range: RefCell::new(None),
                rendered_selected_lines_state: RefCell::new(None),
                rendered_editor_clean: Cell::new(false),
                selection_anchor: Cell::new(None),
                pointer_session: RefCell::new(PointerSession::Idle),
            }
        })
    }

    pub fn options(&self) -> Rc<InteractionManagerOptions<NodeKey>> {
        Rc::clone(&self.options.borrow())
    }

    pub fn set_options(&self, options: InteractionManagerOptions<NodeKey>) {
        *self.options.borrow_mut() = Rc::new(options);
    }

    fn session(&self) -> PointerSession {
        self.pointer_session.borrow().clone()
    }

    pub fn clean_up(&self) {
        let pre = self.pre.borrow_mut().take();
        if let Some(pre) = &pre {
            self.host.remove_pre_listeners(*pre);
            self.host
                .document()
                .remove_attribute(pre, "data-interactive-lines");
            self.host
                .document()
                .remove_attribute(pre, "data-interactive-line-numbers");
        }
        if let Some(container) = self.gutter_utility_container.borrow_mut().take() {
            self.host.document().remove(container);
        }
        *self.gutter_utility_line.borrow_mut() = None;
        *self.gutter_utility_button.borrow_mut() = None;
        *self.gutter_utility_slot.borrow_mut() = None;
        self.clear_hovered_line();
        self.detach_document_pointer_listeners();
        self.clear_pointer_session();
        self.render_queue
            .dequeue_render(&self.render_selection_callback);
        self.interactive_lines_attr.set(false);
        self.interactive_line_numbers_attr.set(false);
        self.has_pointer_listeners.set(false);
        self.set_selection_dirty();
    }

    pub fn setup(&self, pre: &NodeKey) {
        self.set_selection_dirty();
        let options = self.options();
        let uses_custom_gutter_utility = options.uses_custom_gutter_utility.unwrap_or(false);
        let enable_gutter_utility = options.base.enable_gutter_utility.unwrap_or(false);

        let new_container = self.pre.borrow().as_ref() != Some(pre);
        if new_container {
            self.clean_up();
            *self.pre.borrow_mut() = Some(*pre);
        }

        if enable_gutter_utility {
            GutterUtility(self).ensure_gutter_utility_node(uses_custom_gutter_utility);
        } else if self.gutter_utility_container.borrow().is_some() {
            if let Some(container) = self.gutter_utility_container.borrow_mut().take() {
                self.host.document().remove(container);
            }
            *self.gutter_utility_line.borrow_mut() = None;
            *self.gutter_utility_button.borrow_mut() = None;
            *self.gutter_utility_slot.borrow_mut() = None;
            if matches!(self.session(), PointerSession::GutterSelecting { .. }) {
                self.clear_pointer_session();
                self.detach_document_pointer_listeners();
            }
        }

        self.sync_pointer_listeners(pre);
        self.update_interactive_line_attributes();
        self.render_selection();
        GutterUtility(self).place_utility();
    }

    pub fn set_selection_dirty(&self) {
        *self.rendered_selected_lines_state.borrow_mut() = None;
        self.rendered_editor_clean.set(false);
    }

    pub fn is_selection_dirty(&self) -> bool {
        self.rendered_selected_lines_state.borrow().is_none() || !self.rendered_editor_clean.get()
    }

    pub fn set_selection(&self, range: Option<SelectedLineRange>, options: SelectionWriteOptions) {
        let is_range_change = !(*self.selected_range.borrow() == range
            || are_selections_equal(range.as_ref(), self.selected_range.borrow().as_ref()));
        let line_number_only = options.line_number_only;
        let is_style_change = options.active_line_side != self.selected_range_highlight_side.get()
            || line_number_only != self.selected_range_line_number_only.get();
        if !self.is_selection_dirty() && !is_range_change && !is_style_change {
            return;
        }
        *self.proposed_selected_range.borrow_mut() = None;
        *self.selected_range.borrow_mut() = range.clone();
        self.selected_range_highlight_side
            .set(options.active_line_side);
        self.selected_range_line_number_only.set(line_number_only);
        self.render_selection();
        GutterUtility(self).place_utility();
        if is_range_change && options.notify != Some(false) {
            self.notify_selection_committed(range.as_ref());
        }
    }

    pub fn get_selection(&self) -> Option<SelectedLineRange> {
        self.selected_range.borrow().clone()
    }

    pub fn get_hovered_line(&self) -> Option<GetHoveredLineResult> {
        let gutter_utility_line = self
            .gutter_utility_line
            .borrow()
            .clone()
            .or_else(|| self.hovered_line.borrow().clone());
        let line = gutter_utility_line?;
        match (self.mode, line.kind) {
            (InteractionManagerMode::Diff, LineEventKind::DiffLine) => Some(GetHoveredLineResult {
                line_number: line.line_number,
                side: line.annotation_side,
            }),
            (InteractionManagerMode::File, LineEventKind::Line) => Some(GetHoveredLineResult {
                line_number: line.line_number,
                side: None,
            }),
            _ => None,
        }
    }

    pub fn handle_pointer_click(&self, event: &PointerEventData<NodeKey>) -> PointerResponse {
        let options = self.options();
        if options.on_hunk_expand.is_none()
            && options.base.on_line_click.is_none()
            && options.base.on_line_number_click.is_none()
        {
            return PointerResponse::default();
        }
        if options.base.on_gutter_utility_click.is_some()
            && Targets(self).is_gutter_utility_path(&Targets(self).composed_path(event))
        {
            return PointerResponse::default();
        }
        self.handle_click_event(event, &options);
        PointerResponse::default()
    }

    pub fn handle_pointer_move(&self, event: &PointerEventData<NodeKey>) -> PointerResponse {
        // Touch and pen moves are active drag/scroll gestures, not hover. Keeping
        // hover mouse-only prevents content scrolls from leaving stale utilities.
        if event.pointer_type != PointerType::Mouse {
            return PointerResponse::default();
        }

        let options = self.options();
        let line_hover_highlight = options.base.line_hover_highlight.unwrap_or_default();
        let enable_gutter_utility = options.base.enable_gutter_utility.unwrap_or(false);
        if line_hover_highlight == LineHoverHighlight::Disabled
            && !enable_gutter_utility
            && options.base.on_line_enter.is_none()
            && options.base.on_line_leave.is_none()
        {
            return PointerResponse::default();
        }
        // should we perhaps throttle this a bit because move can be fast as fuk
        // boiiii
        self.handle_move_event(event, &options);
        PointerResponse::default()
    }

    pub fn handle_pointer_leave(&self, event: &PointerEventData<NodeKey>) -> PointerResponse {
        if self.hovered_line.borrow().is_none() {
            return PointerResponse::default();
        }
        let options = self.options();
        let hovered_line = self.hovered_line.borrow().clone();
        if let Some(hovered_line) = hovered_line {
            if let Some(on_line_leave) = &options.base.on_line_leave {
                on_line_leave(&LineEventProps {
                    base: hovered_line,
                    event: event.clone(),
                });
            }
            self.clear_hovered_line();
        }
        GutterUtility(self).place_utility();
        PointerResponse::default()
    }

    fn handle_move_event(
        &self,
        event: &PointerEventData<NodeKey>,
        options: &InteractionManagerOptions<NodeKey>,
    ) {
        let target = Targets(self).resolve_pointer_target(&Targets(self).composed_path(event));
        let hoverable = match &target {
            Some(ResolvedPointerTarget::Line(fields)) => Some(fields),
            _ => None,
        };
        let same_line = hoverable.is_some_and(|fields| {
            self.hovered_line
                .borrow()
                .as_ref()
                .is_some_and(|hovered| hovered.line_element == fields.line_element)
        });

        // Handle line transitions
        if !same_line {
            let hovered_line = self.hovered_line.borrow().clone();
            if let Some(hovered_line) = hovered_line {
                if let Some(on_line_leave) = &options.base.on_line_leave {
                    on_line_leave(&LineEventProps {
                        base: hovered_line,
                        event: event.clone(),
                    });
                }
                self.clear_hovered_line();
            }
            if let Some(fields) = hoverable {
                self.set_hovered_line(Targets(self).to_event_base_props(fields));
                GutterUtility(self).place_utility();
                if let Some(on_line_enter) = &options.base.on_line_enter {
                    let base = self
                        .hovered_line
                        .borrow()
                        .clone()
                        .expect("the hovered line was just set");
                    on_line_enter(&LineEventProps {
                        base,
                        event: event.clone(),
                    });
                }
            } else {
                GutterUtility(self).place_utility();
            }
        }
    }

    fn handle_click_event(
        &self,
        event: &PointerEventData<NodeKey>,
        options: &InteractionManagerOptions<NodeKey>,
    ) {
        let target = Targets(self).resolve_pointer_target(&Targets(self).composed_path(event));
        let Some(target) = target else {
            return;
        };
        match target {
            ResolvedPointerTarget::Expando {
                hunk_index,
                direction,
                all,
            } => {
                if let Some(on_hunk_expand) = &options.on_hunk_expand {
                    on_hunk_expand(
                        hunk_index,
                        if all || event.shift_key {
                            ExpansionDirections::Both
                        } else {
                            direction
                        },
                        if all || event.shift_key {
                            Some(f64::INFINITY)
                        } else {
                            None
                        },
                    );
                }
            }
            ResolvedPointerTarget::Line(fields) => {
                let event_base = Targets(self).to_event_base_props(&fields);
                if options.base.on_line_number_click.is_some() && fields.number_column {
                    if let Some(on_line_number_click) = &options.base.on_line_number_click {
                        on_line_number_click(&LineEventProps {
                            base: event_base,
                            event: event.clone(),
                        });
                    }
                } else if let Some(on_line_click) = &options.base.on_line_click {
                    on_line_click(&LineEventProps {
                        base: event_base,
                        event: event.clone(),
                    });
                }
            }
        }
    }

    fn sync_pointer_listeners(&self, pre: &NodeKey) {
        let options = self.options();
        let line_hover_highlight = options.base.line_hover_highlight.unwrap_or_default();
        let enable_gutter_utility = options.base.enable_gutter_utility.unwrap_or(false);
        let enable_line_selection = options.base.enable_line_selection.unwrap_or(false);
        let enable_gutter_selection = options.base.on_gutter_utility_click.is_some();
        let should_attach_pointer_listeners = line_hover_highlight != LineHoverHighlight::Disabled
            || options.base.on_line_click.is_some()
            || options.base.on_line_number_click.is_some()
            || options.base.on_line_enter.is_some()
            || options.base.on_line_leave.is_some()
            || options.on_hunk_expand.is_some()
            || enable_gutter_utility
            || enable_line_selection
            || enable_gutter_selection;

        if should_attach_pointer_listeners && !self.has_pointer_listeners.get() {
            self.host.add_pre_listeners(*pre);
            self.has_pointer_listeners.set(true);
        } else if !should_attach_pointer_listeners && self.has_pointer_listeners.get() {
            self.host.remove_pre_listeners(*pre);
            self.has_pointer_listeners.set(false);
        }

        let session = self.session();
        let has_active_line_selection_session = matches!(
            session,
            PointerSession::Selecting { .. } | PointerSession::PendingSingleLineUnselect { .. }
        );
        let has_active_gutter_selection_session =
            matches!(session, PointerSession::GutterSelecting { .. });
        if (!enable_line_selection && has_active_line_selection_session)
            || (!enable_gutter_selection && has_active_gutter_selection_session)
        {
            self.clear_pointer_session();
            self.detach_document_pointer_listeners();
            self.selection_anchor.set(None);
            self.clear_pending_single_line_state();
        }
    }

    fn update_interactive_line_attributes(&self) {
        let Some(pre) = *self.pre.borrow() else {
            return;
        };
        let options = self.options();
        let should_have_interactive_lines = options.base.on_line_click.is_some();
        let should_have_interactive_line_numbers = options.base.on_line_number_click.is_some()
            || options.base.enable_line_selection.unwrap_or(false);

        if should_have_interactive_lines && !self.interactive_lines_attr.get() {
            self.host
                .document()
                .set_attribute(pre, "data-interactive-lines", "");
            self.interactive_lines_attr.set(true);
        } else if !should_have_interactive_lines && self.interactive_lines_attr.get() {
            self.host
                .document()
                .remove_attribute(pre, "data-interactive-lines");
            self.interactive_lines_attr.set(false);
        }

        if should_have_interactive_line_numbers && !self.interactive_line_numbers_attr.get() {
            self.host
                .document()
                .set_attribute(pre, "data-interactive-line-numbers", "");
            self.interactive_line_numbers_attr.set(true);
        } else if !should_have_interactive_line_numbers && self.interactive_line_numbers_attr.get()
        {
            self.host
                .document()
                .remove_attribute(pre, "data-interactive-line-numbers");
            self.interactive_line_numbers_attr.set(false);
        }
    }

    pub fn handle_pointer_down(&self, event: &PointerEventData<NodeKey>) -> PointerResponse {
        if (event.pointer_type == PointerType::Mouse && event.button != 0)
            || self.pre.borrow().is_none()
            || self.session() != PointerSession::Idle
        {
            return PointerResponse::default();
        }

        let path = Targets(self).composed_path(event);
        if Targets(self).is_gutter_utility_path(&path)
            && self.options().base.on_gutter_utility_click.is_some()
        {
            self.start_gutter_selection_from_pointer_down(event)
        } else {
            if event.pointer_type != PointerType::Mouse {
                GutterUtility(self).reveal_utility_from_gutter_path(&path);
            }
            self.start_line_selection_from_pointer_down(event);
            PointerResponse::default()
        }
    }

    fn start_line_selection_from_pointer_down(&self, event: &PointerEventData<NodeKey>) {
        let options = self.options();
        if !options.base.enable_line_selection.unwrap_or(false) {
            return;
        }

        let Some(pointer_info) = Targets(self).resolve_selection_info(
            event,
            SelectionHitOptions {
                source: SelectionHitSource::EventPath,
                exclude_utility: None,
            },
            true,
        ) else {
            return;
        };

        let Some(pre) = *self.pre.borrow() else {
            return;
        };

        // Intentionally no preventDefault here. The line-number gutter uses
        // `user-select: none`, so a drag that starts on it never begins a native
        // text selection. Calling preventDefault would also cancel the browser's
        // native focus change, which stops a focusable host wrapper from receiving
        // focus (and the keyboard events it needs) when a line number is clicked.
        let SelectionInfo {
            line_number,
            event_side,
            line_index,
        } = pointer_info;

        let selected_range = self.selected_range.borrow().clone();
        if let (true, Some(selected_range)) = (event.shift_key, &selected_range) {
            let Some(row_range) = SelectionRows(self).get_indexes_from_selection(
                selected_range,
                self.host
                    .document()
                    .get_attribute(pre, "data-diff-type")
                    .as_deref()
                    == Some("split"),
            ) else {
                return;
            };
            let use_start = if row_range.0 <= row_range.1 {
                line_index >= row_range.0
            } else {
                line_index <= row_range.1
            };
            self.selection_anchor.set(Some(SelectionPoint {
                line_number: if use_start {
                    selected_range.start
                } else {
                    selected_range.end
                },
                side: if use_start {
                    selected_range.side
                } else {
                    selected_range.end_side.or(selected_range.side)
                },
            }));
            self.update_selection(Some(line_number), event_side, false);
            self.notify_selection_start(self.get_current_selection_range().as_ref());
            *self.pointer_session.borrow_mut() = PointerSession::Selecting {
                pointer_id: event.pointer_id,
            };
            self.attach_document_pointer_listeners();
            return;
        }

        if selected_range
            .as_ref()
            .is_some_and(|range| range.start == line_number && range.end == line_number)
        {
            let point = SelectionPoint {
                line_number,
                side: event_side,
            };
            self.selection_anchor.set(Some(point));
            *self.pointer_session.borrow_mut() = PointerSession::PendingSingleLineUnselect {
                pointer_id: event.pointer_id,
                anchor: point,
                pending: point,
            };
            self.attach_document_pointer_listeners();
            return;
        }

        if options.base.controlled_selection == Some(true) {
            *self.proposed_selected_range.borrow_mut() = Some(None);
        } else {
            *self.selected_range.borrow_mut() = None;
        }
        GutterUtility(self).place_utility();
        self.selection_anchor.set(Some(SelectionPoint {
            line_number,
            side: event_side,
        }));
        self.update_selection(Some(line_number), event_side, false);
        self.notify_selection_start(self.get_current_selection_range().as_ref());
        *self.pointer_session.borrow_mut() = PointerSession::Selecting {
            pointer_id: event.pointer_id,
        };
        self.attach_document_pointer_listeners();
    }

    fn start_gutter_selection_from_pointer_down(
        &self,
        event: &PointerEventData<NodeKey>,
    ) -> PointerResponse {
        if self.options().base.on_gutter_utility_click.is_none() {
            return PointerResponse::default();
        }
        // If a selection already exists, the utility represents that whole range.
        // Dragging should start from the visual top and extend from the visual
        // bottom, regardless of the direction the range was originally created.
        let selected_endpoints = GutterUtility(self).current_selection_ends();
        let point = match &selected_endpoints {
            Some(endpoints) => Some(endpoints.bottom),
            None => Targets(self).resolve_selection_point(
                event,
                SelectionHitOptions {
                    source: SelectionHitSource::EventPath,
                    exclude_utility: Some(false),
                },
            ),
        };
        let anchor = match &selected_endpoints {
            Some(endpoints) => Some(endpoints.top),
            None => point,
        };
        let (Some(point), Some(anchor)) = (point, anchor) else {
            return PointerResponse::default();
        };
        *self.pointer_session.borrow_mut() = PointerSession::GutterSelecting {
            pointer_id: event.pointer_id,
            anchor,
            current: point,
        };
        self.selection_anchor.set(Some(SelectionPoint {
            line_number: anchor.line_number,
            side: anchor.side,
        }));
        self.update_selection(Some(point.line_number), point.side, false);
        self.notify_selection_start(self.get_current_selection_range().as_ref());
        self.attach_document_pointer_listeners();
        PointerResponse {
            prevent_default: true,
            stop_propagation: true,
        }
    }

    pub fn handle_document_pointer_move(
        &self,
        event: &PointerEventData<NodeKey>,
    ) -> PointerResponse {
        let coordinates_first = SelectionHitOptions {
            source: SelectionHitSource::CoordinatesFirst,
            exclude_utility: None,
        };
        match self.session() {
            PointerSession::Idle => PointerResponse::default(),
            PointerSession::GutterSelecting { pointer_id, .. } => {
                if event.pointer_id != pointer_id {
                    return PointerResponse::default();
                }
                let response = PointerResponse {
                    prevent_default: true,
                    stop_propagation: false,
                };
                let Some(point) = Targets(self).resolve_selection_point(event, coordinates_first)
                else {
                    return response;
                };
                if let PointerSession::GutterSelecting { current, .. } =
                    &mut *self.pointer_session.borrow_mut()
                {
                    *current = point;
                }
                self.update_selection(Some(point.line_number), point.side, true);
                response
            }
            PointerSession::Selecting { pointer_id } => {
                if event.pointer_id != pointer_id {
                    return PointerResponse::default();
                }
                let response = PointerResponse {
                    prevent_default: true,
                    stop_propagation: false,
                };
                let Some(pointer_info) =
                    Targets(self).resolve_selection_info(event, coordinates_first, false)
                else {
                    return response;
                };
                if self.selection_anchor.get().is_none() {
                    return response;
                }
                self.update_selection(
                    Some(pointer_info.line_number),
                    pointer_info.event_side,
                    true,
                );
                response
            }
            PointerSession::PendingSingleLineUnselect {
                pointer_id,
                pending,
                ..
            } => {
                if event.pointer_id != pointer_id {
                    return PointerResponse::default();
                }
                let response = PointerResponse {
                    prevent_default: true,
                    stop_propagation: false,
                };
                let Some(pointer_info) =
                    Targets(self).resolve_selection_info(event, coordinates_first, false)
                else {
                    return response;
                };
                if self.selection_anchor.get().is_none() {
                    return response;
                }
                let point = SelectionPoint {
                    line_number: pointer_info.line_number,
                    side: pointer_info.event_side,
                };
                if are_selection_points_equal(&pending, &point) {
                    return response;
                }
                self.update_selection(
                    Some(pointer_info.line_number),
                    pointer_info.event_side,
                    false,
                );
                self.notify_selection_start(self.get_current_selection_range().as_ref());
                self.notify_selection_change_delta();
                *self.pointer_session.borrow_mut() = PointerSession::Selecting {
                    pointer_id: event.pointer_id,
                };
                response
            }
        }
    }

    pub fn handle_document_pointer_up(&self, event: &PointerEventData<NodeKey>) -> PointerResponse {
        let prevented = PointerResponse {
            prevent_default: true,
            stop_propagation: false,
        };
        match self.session() {
            PointerSession::Idle => PointerResponse::default(),
            PointerSession::GutterSelecting {
                pointer_id,
                anchor,
                mut current,
            } => {
                if event.pointer_id != pointer_id {
                    return PointerResponse::default();
                }
                let point = Targets(self).resolve_selection_point(
                    event,
                    SelectionHitOptions {
                        source: SelectionHitSource::CoordinatesFirst,
                        exclude_utility: None,
                    },
                );
                if let Some(point) = point {
                    current = point;
                    self.update_selection(Some(point.line_number), point.side, true);
                }
                // Keep the completed gesture stable when callbacks synchronously
                // replace the manager's controlled or uncontrolled selection.
                let completed_range = Self::build_selected_line_range(&anchor, &current);
                if let Some(on_gutter_utility_click) = &self.options().base.on_gutter_utility_click
                {
                    on_gutter_utility_click(completed_range.clone());
                }
                self.selection_anchor.set(None);
                self.notify_selection_end(Some(&completed_range));
                self.notify_selection_committed(Some(&completed_range));
                self.clear_proposed_selection();
                self.clear_pointer_session();
                self.detach_document_pointer_listeners();
                prevented
            }
            PointerSession::PendingSingleLineUnselect { pointer_id, .. } => {
                if event.pointer_id != pointer_id {
                    return PointerResponse::default();
                }
                self.update_selection(None, None, false);
                self.selection_anchor.set(None);
                self.clear_pending_single_line_state();
                self.detach_document_pointer_listeners();
                self.notify_selection_end(self.get_current_selection_range().as_ref());
                self.notify_selection_committed(self.get_current_selection_range().as_ref());
                self.clear_proposed_selection();
                prevented
            }
            PointerSession::Selecting { pointer_id } => {
                if event.pointer_id != pointer_id {
                    return PointerResponse::default();
                }
                self.selection_anchor.set(None);
                self.detach_document_pointer_listeners();
                self.clear_pointer_session();
                self.notify_selection_end(self.get_current_selection_range().as_ref());
                self.notify_selection_committed(self.get_current_selection_range().as_ref());
                self.clear_proposed_selection();
                prevented
            }
        }
    }

    pub fn handle_document_pointer_cancel(
        &self,
        event: &PointerEventData<NodeKey>,
    ) -> PointerResponse {
        let pointer_id = match self.session() {
            PointerSession::Idle => return PointerResponse::default(),
            PointerSession::GutterSelecting { pointer_id, .. }
            | PointerSession::Selecting { pointer_id }
            | PointerSession::PendingSingleLineUnselect { pointer_id, .. } => pointer_id,
        };
        if event.pointer_id != pointer_id {
            return PointerResponse::default();
        }
        self.selection_anchor.set(None);
        self.clear_proposed_selection();
        self.clear_pending_single_line_state();
        self.clear_pointer_session();
        self.detach_document_pointer_listeners();
        PointerResponse::default()
    }

    fn clear_hovered_line(&self) {
        let Some(hovered_line) = self.hovered_line.borrow_mut().take() else {
            return;
        };
        self.host
            .document()
            .remove_attribute(hovered_line.line_element, "data-hovered");
        self.host
            .document()
            .remove_attribute(hovered_line.number_element, "data-hovered");
    }

    fn set_hovered_line(&self, hovered_line: LineEventBaseProps<NodeKey>) {
        let line_hover_highlight = self.options().base.line_hover_highlight.unwrap_or_default();
        if self.hovered_line.borrow().is_some() {
            self.clear_hovered_line();
        }
        *self.hovered_line.borrow_mut() = Some(hovered_line.clone());
        if line_hover_highlight != LineHoverHighlight::Disabled {
            if matches!(
                line_hover_highlight,
                LineHoverHighlight::Both | LineHoverHighlight::Line
            ) {
                self.host
                    .document()
                    .set_attribute(hovered_line.line_element, "data-hovered", "");
            }
            if matches!(
                line_hover_highlight,
                LineHoverHighlight::Both | LineHoverHighlight::Number
            ) {
                self.host
                    .document()
                    .set_attribute(hovered_line.number_element, "data-hovered", "");
            }
        }
    }

    fn attach_document_pointer_listeners(&self) {
        if self.has_document_pointer_listeners.get() {
            return;
        }
        self.host.add_document_listeners();
        self.has_document_pointer_listeners.set(true);
    }

    fn detach_document_pointer_listeners(&self) {
        if !self.has_document_pointer_listeners.get() {
            return;
        }
        self.host.remove_document_listeners();
        self.has_document_pointer_listeners.set(false);
    }

    fn clear_pointer_session(&self) {
        *self.pointer_session.borrow_mut() = PointerSession::Idle;
    }

    fn clear_pending_single_line_state(&self) {
        if matches!(
            self.session(),
            PointerSession::PendingSingleLineUnselect { .. }
        ) {
            *self.pointer_session.borrow_mut() = PointerSession::Idle;
        }
    }

    fn get_current_selection_range(&self) -> Option<SelectedLineRange> {
        match &*self.proposed_selected_range.borrow() {
            Some(proposed) => proposed.clone(),
            None => self.selected_range.borrow().clone(),
        }
    }

    fn clear_proposed_selection(&self) {
        *self.proposed_selected_range.borrow_mut() = None;
    }

    fn update_selection(
        &self,
        current_line: Option<usize>,
        side: Option<SelectionSide>,
        emit_change: bool,
    ) {
        let previous_range = self.get_current_selection_range();
        let next_range = current_line.map(|current_line| {
            let anchor = self.selection_anchor.get();
            let anchor_side = anchor.and_then(|anchor| anchor.side).or(side);
            let anchor_line = anchor.map_or(current_line, |anchor| anchor.line_number);
            Self::build_selection_range(anchor_line, current_line, anchor_side, side)
        });
        if are_selections_equal(previous_range.as_ref(), next_range.as_ref()) {
            return;
        }
        self.selected_range_highlight_side.set(None);
        self.selected_range_line_number_only.set(false);
        if self.options().base.controlled_selection == Some(true) {
            *self.proposed_selected_range.borrow_mut() = Some(next_range);
        } else {
            *self.selected_range.borrow_mut() = next_range;
            self.render_queue
                .queue_render(&self.render_selection_callback);
        }
        GutterUtility(self).place_utility();
        if emit_change {
            self.notify_selection_change_delta();
        }
    }

    fn render_selection(&self) {
        self.render_queue
            .dequeue_render(&self.render_selection_callback);
        let selected_lines = LineRenderState {
            range: self.selected_range.borrow().clone(),
            highlight_side: self.selected_range_highlight_side.get(),
            line_number_only: self.selected_range_line_number_only.get(),
        };
        let Some(pre) = *self.pre.borrow() else {
            return;
        };
        let selected_lines_changed = !are_line_render_states_equal(
            self.rendered_selected_lines_state.borrow().as_ref(),
            &selected_lines,
        );
        let editor_active_line_changed = !self.rendered_editor_clean.get();
        if !selected_lines_changed && !editor_active_line_changed {
            return;
        }

        // A changed channel remains explicitly dirty until its DOM update finishes,
        // which lets an identical write retry after a transient render failure.
        if selected_lines_changed {
            *self.rendered_selected_lines_state.borrow_mut() = None;
        }
        if editor_active_line_changed {
            self.rendered_editor_clean.set(false);
        }

        let split = self
            .host
            .document()
            .get_attribute(pre, "data-diff-type")
            .as_deref()
            == Some("split");
        let selected_row_range = if selected_lines_changed {
            SelectionRows(self).resolve_line_render_range(&selected_lines, split)
        } else {
            None
        };
        let columns = if selected_lines_changed {
            SelectionRows(self).get_line_render_columns(&selected_lines, selected_row_range)
        } else {
            Vec::new()
        };
        let selected_lines_complete = selected_lines.range.is_none() || !columns.is_empty();

        if selected_lines_changed {
            for element in self
                .host
                .document()
                .query_by_attribute(pre, "data-selected-line", None)
            {
                self.host
                    .document()
                    .remove_attribute(element, "data-selected-line");
            }
        }

        if let Some((start, end)) = selected_row_range {
            let selected_first = start.min(end);
            let selected_last = start.max(end);
            for column in &columns {
                SelectionRows(self).render_selected_column(
                    column,
                    selected_first,
                    selected_last,
                    &selected_lines,
                    split,
                );
            }
        }

        if selected_lines_changed && selected_lines_complete {
            *self.rendered_selected_lines_state.borrow_mut() = Some(selected_lines);
        }
        // The editor active line channel is dropped: it is clean once rendered.
        if editor_active_line_changed {
            self.rendered_editor_clean.set(true);
        }
    }

    fn notify_selection_committed(&self, range: Option<&SelectedLineRange>) {
        if let Some(callback) = &self.options().base.on_line_selected {
            callback(range);
        }
    }

    fn notify_selection_change_delta(&self) {
        if let Some(callback) = &self.options().base.on_line_selection_change {
            callback(self.get_current_selection_range().as_ref());
        }
    }

    fn notify_selection_start(&self, range: Option<&SelectedLineRange>) {
        if let Some(callback) = &self.options().base.on_line_selection_start {
            callback(range);
        }
    }

    fn notify_selection_end(&self, range: Option<&SelectedLineRange>) {
        if let Some(callback) = &self.options().base.on_line_selection_end {
            callback(range);
        }
    }

    fn build_selected_line_range(
        anchor: &SelectionPoint,
        current: &SelectionPoint,
    ) -> SelectedLineRange {
        Self::build_selection_range(
            anchor.line_number,
            current.line_number,
            anchor.side,
            current.side,
        )
    }

    fn build_selection_range(
        start: usize,
        end: usize,
        side: Option<SelectionSide>,
        end_side: Option<SelectionSide>,
    ) -> SelectedLineRange {
        SelectedLineRange {
            start,
            end,
            side,
            end_side: if side != end_side { end_side } else { None },
        }
    }
}

// `first?.x === second.x` with nothing rendered yet compares `undefined` with
// `false` for `lineNumberOnly`, so "nothing rendered" is never equal.
fn are_line_render_states_equal(first: Option<&LineRenderState>, second: &LineRenderState) -> bool {
    first.is_some_and(|first| first == second)
}

// `/^annotation-(?:(additions|deletions)-)?(\d+)$/`
fn selection_point_from_annotation_slot_name(slot_name: Option<&str>) -> Option<SelectionPoint> {
    let rest = slot_name?.strip_prefix("annotation-")?;
    let (side, digits) = if let Some(digits) = rest.strip_prefix("additions-") {
        (Some(SelectionSide::Additions), digits)
    } else if let Some(digits) = rest.strip_prefix("deletions-") {
        (Some(SelectionSide::Deletions), digits)
    } else {
        (None, rest)
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let line_number: usize = digits.parse().ok()?;
    if line_number == 0 {
        return None;
    }
    Some(SelectionPoint { line_number, side })
}
