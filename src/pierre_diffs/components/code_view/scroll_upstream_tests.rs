// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/CodeView.rangeScroll.test.ts
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; file items become diff items (divergences.psv).

//! Upstream's `test/CodeView.rangeScroll.test.ts` (C3, whole in C5), ported
//! with its names (prefixed by its `describe`): each case runs `setup(root)`,
//! `renderItems`, `scrollTo` and `render(true)` and reads `root.scrollTop`. The
//! cases that scroll a file item (`makeFileItem`, DEFER with `VirtualizedFile`)
//! use a diff item with `expandUnchanged` (every line rendered), so the expected
//! positions are computed from `get_line_position`, the model's own line
//! geometry (parity/divergences.psv), not from `getFileLineTop`'s file
//! arithmetic. The two diff cases are as upstream's.
//! `a_nearest_range_ending_below_the_viewport_aligns_to_its_end` has no
//! upstream counterpart (the `end` half of `nearest`). Test-only.

use std::rc::Rc;

use super::harness_tests::Page;
use super::harness_tests::diff_item;
use super::harness_tests::line_target;
use super::harness_tests::lines;
use super::harness_tests::page;
use super::harness_tests::position_target;
use super::harness_tests::range_target;
use super::options::CodeViewOptions;
use crate::constants::DEFAULT_CODE_VIEW_FILE_METRICS;
use crate::types::options::DiffStyle;
use crate::types::view::AnnotationSide;
use crate::types::view::CodeViewItem;
use crate::types::view::ScrollAlign;

const ROOT_HEIGHT: f64 = 800.0;

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

/// A viewer set up on an `ROOT_HEIGHT` root over `item`, every line rendered
/// (`expandUnchanged`) in `diff_style`.
fn viewer(item: CodeViewItem<u8>, diff_style: DiffStyle) -> Page {
    let mut options = CodeViewOptions::default();
    options.diff.base.diff_style = Some(diff_style);
    let page = page(options, ROOT_HEIGHT);
    page.render_items(&[item]);
    page
}

/// `viewer.scrollTo(target); viewer.render(true); root.scrollTop`
fn scroll_to(page: &Page, target: &crate::types::view::CodeViewScrollTarget) -> f64 {
    page.view.scroll_to(target);
    page.view.render(true);
    page.scroll_top()
}

/// `getViewportTopForLocalTop`: an item-local top as an absolute scroll top.
fn viewport_top(page: &Page, id: &str, line_number: usize) -> (f64, f64) {
    let item = page.view.item_by_id(id).expect("the item exists");
    let instance = Rc::clone(item.borrow().instance());
    let position = instance
        .get_line_position(line_number, AnnotationSide::Additions)
        .expect("the line has a position");
    (
        page.view.get_item_top_offset() + item.borrow().top + position.top,
        position.height,
    )
}

// ---- CodeView range scrolling -----------------------------------------------------

#[test]
fn code_view_range_scrolling_scrolls_a_single_line_range_to_the_same_position_as_a_line_target() {
    let page = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);

    let line_scroll_top = scroll_to(
        &page,
        &line_target("diff:example", 50, None, ScrollAlign::Center),
    );
    // Anchor the equivalence on the exact derived center position so it cannot
    // pass vacuously with both scroll paths silently no-oping at 0.
    let (top, height) = viewport_top(&page, "diff:example", 50);
    assert_eq!(line_scroll_top, top - (ROOT_HEIGHT - height) / 2.0);
    assert!(line_scroll_top > 0.0);

    scroll_to(&page, &position_target(0.0));
    let range_scroll_top = scroll_to(
        &page,
        &range_target("diff:example", 50, 50, None, ScrollAlign::Center),
    );

    assert_eq!(range_scroll_top, line_scroll_top);
    page.finish();
}

#[test]
fn code_view_range_scrolling_centers_a_multi_line_range_as_a_single_region() {
    let page = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);

    let scroll_top = scroll_to(
        &page,
        &range_target("diff:example", 20, 30, None, ScrollAlign::Center),
    );

    let (range_top, _) = viewport_top(&page, "diff:example", 20);
    let (last_top, last_height) = viewport_top(&page, "diff:example", 30);
    let range_height = last_top + last_height - range_top;
    assert_eq!(
        range_height,
        11.0 * DEFAULT_CODE_VIEW_FILE_METRICS.line_height
    );
    assert_eq!(scroll_top, range_top - (ROOT_HEIGHT - range_height) / 2.0);
    page.finish();
}

#[test]
fn code_view_range_scrolling_keeps_nearest_alignment_still_when_the_full_range_is_visible() {
    let page = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);
    page.scroll_and_render(500.0);
    let (top, _) = viewport_top(&page, "diff:example", 30);
    // The range (lines 30 to 35) sits inside [500, 1300).
    assert!(
        top > 500.0 && top < 1300.0,
        "the fixture puts line 30 at {top}"
    );

    let scroll_top = scroll_to(
        &page,
        &range_target("diff:example", 30, 35, None, ScrollAlign::Nearest),
    );

    assert_eq!(scroll_top, 500.0);
    page.finish();
}

#[test]
fn code_view_range_scrolling_moves_nearest_alignment_when_the_range_starts_above_the_viewport() {
    let page = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);
    page.scroll_and_render(1500.0);

    let scroll_top = scroll_to(
        &page,
        &range_target("diff:example", 30, 35, None, ScrollAlign::Nearest),
    );

    let (top, _) = viewport_top(&page, "diff:example", 30);
    assert_eq!(scroll_top, top);
    page.finish();
}

#[test]
fn a_nearest_range_ending_below_the_viewport_aligns_to_its_end() {
    let page = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);

    let scroll_top = scroll_to(
        &page,
        &range_target("diff:example", 60, 65, None, ScrollAlign::Nearest),
    );

    let (end_top, end_height) = viewport_top(&page, "diff:example", 65);
    assert_eq!(scroll_top, end_top + end_height - ROOT_HEIGHT);
    page.finish();
}

#[test]
fn code_view_range_scrolling_falls_back_to_start_alignment_when_a_centered_range_is_taller_than_the_viewport()
 {
    let page = viewer(inserted_diff_item("diff:example"), DiffStyle::Unified);

    let scroll_top = scroll_to(
        &page,
        &range_target("diff:example", 10, 60, None, ScrollAlign::Center),
    );

    let (top, _) = viewport_top(&page, "diff:example", 10);
    assert_eq!(scroll_top, top);
    page.finish();
}

#[test]
fn code_view_range_scrolling_resolves_split_view_range_endpoints_against_their_requested_sides() {
    let page = viewer(inserted_diff_item("diff:inserted"), DiffStyle::Split);

    let additions_scroll_top = scroll_to(
        &page,
        &range_target(
            "diff:inserted",
            120,
            120,
            Some(AnnotationSide::Additions),
            ScrollAlign::Center,
        ),
    );

    scroll_to(&page, &position_target(0.0));
    let deletions_scroll_top = scroll_to(
        &page,
        &range_target(
            "diff:inserted",
            120,
            120,
            Some(AnnotationSide::Deletions),
            ScrollAlign::Center,
        ),
    );

    assert_eq!(
        deletions_scroll_top - additions_scroll_top,
        10.0 * DEFAULT_CODE_VIEW_FILE_METRICS.line_height
    );
    page.finish();
}

#[test]
fn code_view_range_scrolling_places_a_context_zero_deletion_between_adjacent_addition_side_lines() {
    let page = viewer(deleted_diff_item("diff:deleted"), DiffStyle::Split);

    let before_deletion_scroll_top = scroll_to(
        &page,
        &line_target(
            "diff:deleted",
            80,
            Some(AnnotationSide::Additions),
            ScrollAlign::Center,
        ),
    );

    let scroll_top = scroll_to(
        &page,
        &line_target(
            "diff:deleted",
            81,
            Some(AnnotationSide::Additions),
            ScrollAlign::Center,
        ),
    );

    assert_eq!(
        scroll_top - before_deletion_scroll_top,
        2.0 * DEFAULT_CODE_VIEW_FILE_METRICS.line_height
    );
    page.finish();
}
