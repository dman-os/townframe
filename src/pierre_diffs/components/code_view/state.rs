// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C2: the interfaces of lines 79-166, 216-263, 678-705 and the field
// initialisers of 770-890).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * `AdvancedVirtualizedBaseItem` is the fields of `ContextItem`; the
//   `CodeViewDiffItemContext | CodeViewFileItemContext` union is `ItemKind`.
//   The file arm holds `FileItemContext`, an uninhabited type: file items need
//   `VirtualizedFile`, which is DEFER (closure notes, Q1), so no file item
//   can exist and every `match` on the arm is `match never {}`; `createItem`
//   reaches it with a `todo!`.
// * An item is shared (`items`, `idToItem`, `instanceToItem` all point at the
//   same record), so it is an `Rc<RefCell<ContextItem>>`. No `RefCell` borrow
//   is held across a call into an instance: instances call back into the
//   viewer (`instanceChanged`).
// * `item.element` is the node key of the pooled shell (`Option<NodeKey>`);
//   nothing sets it before C4's element pool.
// * `CodeViewItemOptionsState` is `ItemOptionsState` (the id the callback
//   wrappers resolve the item by, shared with `updateItemId`); the edit-mode
//   `removedItem` field is DROP. `callbackCache` is `ItemCallbackCache`, in
//   `item_options.rs`.
// * `PendingScrollTarget` and `ScrollAnchor` are data here (`updateItemId`
//   renames the ids they hold); C3 owns the math over them.
// * `renderedOptionsRevision` stays signed: `syncItemRecord` stores `-1` to
//   force the next render.

use std::cell::RefCell;
use std::rc::Rc;

use super::item_options::ItemCallbackCache;
use super::options::RenderCodeViewSlotCallback;
use crate::components::file_diff::RenderedContent;
use crate::components::virtualized_file_diff::instance::VirtualizedFileDiff;
use crate::document::NodeKey;
use crate::types::layout::VirtualWindowSpecs;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::CodeViewItem;
use crate::types::view::CodeViewItemScrollTarget;
use crate::types::view::CodeViewLineScrollTarget;
use crate::types::view::CodeViewPositionScrollTarget;
use crate::types::view::CodeViewRangeScrollTarget;
use crate::types::view::ScrollAlign;
use crate::types::view::SelectedLineRange;
use crate::types::view::SelectionSide;

/// `CodeViewLineSelection`
#[derive(Debug, Clone, PartialEq)]
pub struct CodeViewLineSelection {
    pub id: String,
    pub range: SelectedLineRange,
}

/// `ItemAnchor`: when re-rendering, the first fully visible item whose offset
/// from the viewport top is restored.
#[derive(Debug, Clone, PartialEq)]
pub struct ItemAnchor {
    pub id: String,
    pub viewport_offset: f64,
}

/// `LineAnchor`: as `ItemAnchor`, at line granularity.
#[derive(Debug, Clone, PartialEq)]
pub struct LineAnchor {
    pub id: String,
    pub line_number: usize,
    pub side: Option<SelectionSide>,
    pub viewport_offset: f64,
}

/// `ScrollAnchor = ItemAnchor | LineAnchor`
#[derive(Debug, Clone, PartialEq)]
pub enum ScrollAnchor {
    Item(ItemAnchor),
    Line(LineAnchor),
}

impl ScrollAnchor {
    pub fn id(&self) -> &str {
        match self {
            Self::Item(anchor) => &anchor.id,
            Self::Line(anchor) => &anchor.id,
        }
    }

    pub fn set_id(&mut self, id: String) {
        match self {
            Self::Item(anchor) => anchor.id = id,
            Self::Line(anchor) => anchor.id = id,
        }
    }
}

/// `PendingAlignTypes`: `Exclude<CodeViewLineScrollTarget['align'], 'nearest'>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingAlign {
    Start,
    Center,
    End,
}

impl From<PendingAlign> for ScrollAlign {
    fn from(align: PendingAlign) -> Self {
        match align {
            PendingAlign::Start => Self::Start,
            PendingAlign::Center => Self::Center,
            PendingAlign::End => Self::End,
        }
    }
}

/// `PendingLineTarget`: a line target whose `nearest` align is resolved.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingLineTarget {
    pub target: CodeViewLineScrollTarget,
    pub align: Option<PendingAlign>,
}

/// `PendingRangeTarget`
#[derive(Debug, Clone, PartialEq)]
pub struct PendingRangeTarget {
    pub target: CodeViewRangeScrollTarget,
    pub align: Option<PendingAlign>,
}

/// `PendingItemTarget`
#[derive(Debug, Clone, PartialEq)]
pub struct PendingItemTarget {
    pub target: CodeViewItemScrollTarget,
    pub align: Option<PendingAlign>,
}

/// `PendingScrollTarget`
#[derive(Debug, Clone, PartialEq)]
pub enum PendingScrollTarget {
    Position(CodeViewPositionScrollTarget),
    Line(PendingLineTarget),
    Range(PendingRangeTarget),
    Item(PendingItemTarget),
}

impl PendingScrollTarget {
    /// The item the target names; `None` for a position.
    pub fn id(&self) -> Option<&str> {
        match self {
            Self::Position(_) => None,
            Self::Line(target) => Some(&target.target.id),
            Self::Range(target) => Some(&target.target.id),
            Self::Item(target) => Some(&target.target.id),
        }
    }

    /// `{ ...pendingScrollTarget, id: newId }`
    pub fn with_id(&self, id: String) -> Self {
        let mut renamed = self.clone();
        match &mut renamed {
            Self::Position(_) => {}
            Self::Line(target) => target.target.id = id,
            Self::Range(target) => target.target.id = id,
            Self::Item(target) => target.target.id = id,
        }
        renamed
    }
}

/// `ScrollToAnimation`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScrollToAnimation {
    pub position: f64,
    pub velocity: f64,
    pub last_timestamp: f64,
}

/// `HeaderFooterHost`: the always-rendered, non-virtualized header or footer
/// host: its element, the last-used render callback and the measured height.
pub struct HeaderFooterHost<Rendered> {
    pub element: Option<NodeKey>,
    pub render: Option<RenderCodeViewSlotCallback<Rendered>>,
    pub height: f64,
}

impl<Rendered> Default for HeaderFooterHost<Rendered> {
    fn default() -> Self {
        Self {
            element: None,
            render: None,
            height: 0.0,
        }
    }
}

/// `renderState`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RenderState {
    pub scroll_top: f64,
    pub first_index: i64,
    pub last_index: i64,
    pub sticky_height: f64,
    pub sticky_top: f64,
    pub sticky_bottom: f64,
}

impl RenderState {
    /// `resetRenderState`'s values, also the field initialiser.
    pub const RESET: Self = Self {
        scroll_top: -1.0,
        first_index: -1,
        last_index: -1,
        sticky_height: 0.0,
        sticky_top: -1.0,
        sticky_bottom: -1.0,
    };
}

/// `windowSpecs`' initialiser.
pub const EMPTY_WINDOW_SPECS: VirtualWindowSpecs = VirtualWindowSpecs {
    top: 0.0,
    bottom: 0.0,
};

/// `CodeViewItemOptionsState`: the id the item's callback wrappers resolve the
/// item by, kept current by `updateItemId`. The id rather than the item so
/// item -> instance -> options does not cycle back to the item.
pub struct ItemOptionsState {
    pub id: RefCell<String>,
}

/// `CodeViewFileItemContext`: uninhabited, see the header.
pub enum FileItemContext {}

/// `CodeViewDiffItemContext`'s own fields.
pub struct DiffItemContext<Host, Rendered, LAnnotation>
where
    Host: super::CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// Latest item snapshot for this record. Controlled updates can replace it.
    pub item: CodeViewDiffItem<LAnnotation>,
    /// Virtualized diff instance responsible for rendering this item.
    pub instance: Rc<VirtualizedFileDiff<Host, Rendered, LAnnotation>>,
    pub options_state: Rc<ItemOptionsState>,
    pub callbacks: ItemCallbackCache<Rendered, LAnnotation>,
}

/// `CodeViewDiffItemContext | CodeViewFileItemContext`
// The diff arm only crosses clippy's size threshold with 64-bit pointers (Rc and Vec
// fields), so on wasm32 the lint does not fire and a bare `expect` would be unfulfilled.
#[cfg_attr(
    target_pointer_width = "64",
    expect(
        clippy::large_enum_variant,
        reason = "the file arm is uninhabited, so the size difference is the diff arm's"
    )
)]
pub enum ItemKind<Host, Rendered, LAnnotation>
where
    Host: super::CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    Diff(DiffItemContext<Host, Rendered, LAnnotation>),
    File(FileItemContext),
}

/// `CodeViewContextItem`: `AdvancedVirtualizedBaseItem` and its variant.
pub struct ContextItem<Host, Rendered, LAnnotation>
where
    Host: super::CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// Current index of this record in the ordered items array.
    pub index: usize,
    /// Absolute top offset of this item inside the scroll content.
    pub top: f64,
    /// Total measured height reserved for this item.
    pub height: f64,
    /// Root `<diffs-container>` node currently mounted for this item, only
    /// exists when rendered.
    pub element: Option<NodeKey>,
    /// Last controlled version observed for this record.
    pub version: Option<usize>,
    /// Last CodeView option revision this item rendered with.
    pub rendered_options_revision: i64,
    pub kind: ItemKind<Host, Rendered, LAnnotation>,
}

pub type ItemRef<Host, Rendered, LAnnotation> =
    Rc<RefCell<ContextItem<Host, Rendered, LAnnotation>>>;

impl<Host, Rendered, LAnnotation> ContextItem<Host, Rendered, LAnnotation>
where
    Host: super::CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `item.item.id`
    pub fn id(&self) -> &str {
        match &self.kind {
            ItemKind::Diff(diff) => &diff.item.id,
            ItemKind::File(never) => match *never {},
        }
    }

    /// `item.item.type === input.type`
    pub fn has_type_of(&self, input: &CodeViewItem<LAnnotation>) -> bool {
        match (&self.kind, input) {
            (ItemKind::Diff(_), CodeViewItem::Diff(_)) => true,
            (ItemKind::File(never), _) => match *never {},
            (ItemKind::Diff(_), CodeViewItem::File(_)) => false,
        }
    }

    /// The latest snapshot of the item (`item.item`).
    pub fn snapshot(&self) -> CodeViewItem<LAnnotation> {
        match &self.kind {
            ItemKind::Diff(diff) => CodeViewItem::Diff(diff.item.clone()),
            ItemKind::File(never) => match *never {},
        }
    }

    /// `instanceToItem`'s key for `item.instance`: the instance's allocation,
    /// what `virtualizer::instance_key` computes from the `InstanceRef` the
    /// instance names itself by.
    pub fn instance_key(&self) -> usize {
        Rc::as_ptr(self.instance()).cast::<()>() as usize
    }

    /// `item.instance`, as the virtualizer holds it.
    pub fn instance(&self) -> &Rc<VirtualizedFileDiff<Host, Rendered, LAnnotation>> {
        match &self.kind {
            ItemKind::Diff(diff) => &diff.instance,
            ItemKind::File(never) => match *never {},
        }
    }
}
