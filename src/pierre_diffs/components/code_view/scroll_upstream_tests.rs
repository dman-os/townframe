//! Upstream's `test/CodeView.rangeScroll.test.ts`, the part that is the scroll
//! model (C3), ported with its names. Every upstream case runs `setup(root)`,
//! `scrollTo` and `render(true)`, then reads `root.scrollTop`: setup is C4 and
//! `scrollTo` and the render pass are C5. What these tests keep is the
//! position the case expects the scroll to land on, which is what
//! `resolve_scroll_target_top` (and `normalize_scroll_target` for `nearest`)
//! answers for the root's size and scroll position; the cases come back whole
//! in C5. The cases that scroll a file item (`makeFileItem`, DEFER with
//! `VirtualizedFile`) use a diff item with `expandUnchanged` (every line
//! rendered), so the expected positions are computed from
//! `get_line_position`, the model's own line geometry (parity/divergences.psv),
//! not from `getFileLineTop`'s file arithmetic. The two diff cases are as
//! upstream's. `a_nearest_range_ending_below_the_viewport_aligns_to_its_end`
//! has no upstream counterpart (the `end` half of `nearest`). Test-only.

use std::rc::Rc;

use super::CodeView;
use super::options::CodeViewOptions;
use super::state::PendingAlign;
use super::state::PendingScrollTarget;
use crate::components::virtualized_file_diff::layout_upstream_tests::plain_file;
use crate::constants::DEFAULT_CODE_VIEW_FILE_METRICS;
use crate::diff::PatchOptions;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::options::DiffStyle;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::ParseDiffOptions;
use crate::types::view::AnnotationSide;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::CodeViewItem;
use crate::types::view::CodeViewLineScrollTarget;
use crate::types::view::CodeViewRangeScrollTarget;
use crate::types::view::CodeViewScrollTarget;
use crate::types::view::ScrollAlign;
use crate::types::view::SelectedLineRange;
use crate::utils::parse_diff_from_file::parse_diff_from_file;

type View = CodeView<FakeDom, u8, u8>;

const ROOT_HEIGHT: f64 = 800.0;

fn lines(count: usize, prefix: &str) -> Vec<String> {
    (1..=count)
        .map(|index| format!("{prefix}{index}"))
        .collect()
}

fn diff_item(id: &str, old: &[String], new: &[String], context: Option<usize>) -> CodeViewItem<u8> {
    let file = |contents: &[String]| -> FileContents {
        plain_file("src/example.ts", &contents.join("\n"))
    };
    let parsed = parse_diff_from_file(
        &DiffFileInput::Both {
            old_file: file(old),
            new_file: file(new),
        },
        &ParseDiffOptions {
            create_patch: PatchOptions {
                context,
                ..PatchOptions::default()
            },
            ..ParseDiffOptions::default()
        },
        false,
    )
    .expect("the files diff");
    CodeViewItem::Diff(CodeViewDiffItem {
        id: id.to_owned(),
        file_diff: Rc::new(parsed.value),
        annotations: None,
        version: None,
        collapsed: None,
        edit: None,
    })
}

/// `makeInsertedDiffItem`: 160 lines with ten inserted after line 80.
fn inserted_diff_item(id: &str) -> CodeViewItem<u8> {
    let old: Vec<String> = (1..=160).map(|index| format!("line {index}")).collect();
    let mut new = old[..80].to_vec();
    new.extend(lines(10, "inserted "));
    new.extend_from_slice(&old[80..]);
    diff_item(id, &old, &new, None)
}

/// `makeDeletedDiffItem`: 160 lines with line 81 removed, context zero.
fn deleted_diff_item(id: &str) -> CodeViewItem<u8> {
    let old: Vec<String> = (1..=160).map(|index| format!("line {index}")).collect();
    let mut new = old.clone();
    new.remove(80);
    diff_item(id, &old, &new, Some(0))
}

/// A viewer with an `ROOT_HEIGHT` root laid out over `item`, every line
/// rendered (`expandUnchanged`) in `diff_style`.
fn viewer(item: CodeViewItem<u8>, diff_style: DiffStyle) -> (Rc<FakeDom>, Rc<View>) {
    let mut options = CodeViewOptions::default();
    options.diff.base.expand_unchanged = Some(true);
    options.diff.base.diff_style = Some(diff_style);
    let dom = Rc::new(FakeDom::default());
    let view = CodeView::new(
        Rc::clone(&dom),
        ResizeObserverHub::new(Rc::clone(&dom)),
        UniversalRenderingManager::new(Rc::clone(&dom)),
        Rc::new(PlainTextHighlighter),
        options,
    );
    view.set_items(&[item]);
    // The layout half of `computeRenderRangeAndEmit`: appending to an empty
    // list lays the item out as it lands, so there may be nothing dirty.
    let pending = {
        let mut state = view.state.borrow_mut();
        let reset = state.pending_layout_reset.take();
        state.layout_dirty_index.take().map(|index| (index, reset))
    };
    if let Some((index, reset)) = pending {
        view.recompute_layout(index, reset);
    }
    let root = dom.doc.create_element("div");
    dom.heights.borrow_mut().insert(root, ROOT_HEIGHT);
    view.state.borrow_mut().root = Some(root);
    (dom, view)
}

fn set_scroll_top(dom: &FakeDom, view: &View, scroll_top: f64) {
    dom.world.borrow_mut().scroll_top = scroll_top;
    view.state.borrow_mut().scroll_dirty = true;
}

fn line_target(
    id: &str,
    line_number: usize,
    side: Option<AnnotationSide>,
    align: ScrollAlign,
) -> CodeViewScrollTarget {
    CodeViewScrollTarget::Line(CodeViewLineScrollTarget {
        id: id.to_owned(),
        line_number,
        side,
        align: Some(align),
        offset: None,
        behavior: None,
    })
}

fn range_target(
    id: &str,
    start: usize,
    end: usize,
    side: Option<AnnotationSide>,
    align: ScrollAlign,
) -> CodeViewScrollTarget {
    CodeViewScrollTarget::Range(CodeViewRangeScrollTarget {
        id: id.to_owned(),
        range: SelectedLineRange {
            start,
            side,
            end,
            end_side: None,
        },
        align: Some(align),
        offset: None,
        behavior: None,
    })
}

/// Where the scroll lands for a (non-`nearest`) target.
fn destination(view: &View, target: &CodeViewScrollTarget) -> f64 {
    let pending = view
        .normalize_scroll_target(target)
        .expect("a non-nearest target is pending");
    view.resolve_scroll_target_top(&pending)
        .expect("the target resolves")
}

/// `getViewportTopForLocalTop`: an item-local top as an absolute scroll top.
fn viewport_top(view: &View, id: &str, line_number: usize) -> (f64, f64) {
    let item = view.item_by_id(id).expect("the item exists");
    let instance = Rc::clone(item.borrow().instance());
    let position = instance
        .get_line_position(line_number, AnnotationSide::Additions)
        .expect("the line has a position");
    (
        view.get_item_top_offset() + item.borrow().top + position.top,
        position.height,
    )
}

// ---- CodeView range scrolling -----------------------------------------------------

#[test]
fn code_view_range_scrolling_scrolls_a_single_line_range_to_the_same_position_as_a_line_target() {
    let (_dom, view) = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);

    let line = destination(
        &view,
        &line_target("diff:example", 50, None, ScrollAlign::Center),
    );
    // Anchor the equivalence on the exact derived center position so it cannot
    // pass vacuously with both paths answering 0.
    let (top, height) = viewport_top(&view, "diff:example", 50);
    assert_eq!(line, top - (ROOT_HEIGHT - height) / 2.0);
    assert!(line > 0.0);

    let range = destination(
        &view,
        &range_target("diff:example", 50, 50, None, ScrollAlign::Center),
    );
    assert_eq!(range, line);
}

#[test]
fn code_view_range_scrolling_centers_a_multi_line_range_as_a_single_region() {
    let (_dom, view) = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);

    let range = destination(
        &view,
        &range_target("diff:example", 20, 30, None, ScrollAlign::Center),
    );

    let (range_top, _) = viewport_top(&view, "diff:example", 20);
    let (last_top, last_height) = viewport_top(&view, "diff:example", 30);
    let range_height = last_top + last_height - range_top;
    assert_eq!(
        range_height,
        11.0 * DEFAULT_CODE_VIEW_FILE_METRICS.line_height
    );
    assert_eq!(range, range_top - (ROOT_HEIGHT - range_height) / 2.0);
}

#[test]
fn code_view_range_scrolling_keeps_nearest_alignment_still_when_the_full_range_is_visible() {
    let (dom, view) = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);
    set_scroll_top(&dom, &view, 500.0);
    let (top, _) = viewport_top(&view, "diff:example", 30);
    // The range (lines 30 to 35) sits inside [500, 1300).
    assert!(
        top > 500.0 && top < 1300.0,
        "the fixture puts line 30 at {top}"
    );

    let pending = view.normalize_scroll_target(&range_target(
        "diff:example",
        30,
        35,
        None,
        ScrollAlign::Nearest,
    ));
    assert_eq!(pending, None);
}

#[test]
fn code_view_range_scrolling_moves_nearest_alignment_when_the_range_starts_above_the_viewport() {
    let (dom, view) = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);
    set_scroll_top(&dom, &view, 1500.0);

    let target = range_target("diff:example", 30, 35, None, ScrollAlign::Nearest);
    let pending = view
        .normalize_scroll_target(&target)
        .expect("the range is above the viewport");
    let PendingScrollTarget::Range(range) = &pending else {
        panic!("a range target stays a range target");
    };
    assert_eq!(range.align, Some(PendingAlign::Start));
    let (top, _) = viewport_top(&view, "diff:example", 30);
    assert_eq!(view.resolve_scroll_target_top(&pending), Some(top));
}

#[test]
fn a_nearest_range_ending_below_the_viewport_aligns_to_its_end() {
    let (_dom, view) = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);

    let target = range_target("diff:example", 60, 65, None, ScrollAlign::Nearest);
    let pending = view
        .normalize_scroll_target(&target)
        .expect("the range is below the viewport");
    let PendingScrollTarget::Range(range) = &pending else {
        panic!("a range target stays a range target");
    };
    assert_eq!(range.align, Some(PendingAlign::End));
    let (end_top, end_height) = viewport_top(&view, "diff:example", 65);
    assert_eq!(
        view.resolve_scroll_target_top(&pending),
        Some(end_top + end_height - ROOT_HEIGHT)
    );
}

#[test]
fn code_view_range_scrolling_falls_back_to_start_alignment_when_a_centered_range_is_taller_than_the_viewport()
 {
    let (_dom, view) = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);

    let range = destination(
        &view,
        &range_target("diff:example", 10, 60, None, ScrollAlign::Center),
    );

    let (top, _) = viewport_top(&view, "diff:example", 10);
    assert_eq!(range, top);
}

#[test]
fn code_view_range_scrolling_resolves_split_view_range_endpoints_against_their_requested_sides() {
    let (_dom, view) = viewer(inserted_diff_item("diff:inserted"), DiffStyle::Split);

    let additions = destination(
        &view,
        &range_target(
            "diff:inserted",
            120,
            120,
            Some(AnnotationSide::Additions),
            ScrollAlign::Center,
        ),
    );
    let deletions = destination(
        &view,
        &range_target(
            "diff:inserted",
            120,
            120,
            Some(AnnotationSide::Deletions),
            ScrollAlign::Center,
        ),
    );

    assert_eq!(
        deletions - additions,
        10.0 * DEFAULT_CODE_VIEW_FILE_METRICS.line_height
    );
}

#[test]
fn code_view_range_scrolling_places_a_context_zero_deletion_between_adjacent_addition_side_lines() {
    let (_dom, view) = viewer(deleted_diff_item("diff:deleted"), DiffStyle::Split);

    let before_deletion = destination(
        &view,
        &line_target(
            "diff:deleted",
            80,
            Some(AnnotationSide::Additions),
            ScrollAlign::Center,
        ),
    );
    let after_deletion = destination(
        &view,
        &line_target(
            "diff:deleted",
            81,
            Some(AnnotationSide::Additions),
            ScrollAlign::Center,
        ),
    );

    assert_eq!(
        after_deletion - before_deletion,
        2.0 * DEFAULT_CODE_VIEW_FILE_METRICS.line_height
    );
}
