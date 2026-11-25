// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/CodeView.scrollAnchoring.test.ts
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; file items become diff items (divergences.psv).

//! Upstream's `test/CodeView.scrollAnchoring.test.ts` (C5), ported with its
//! names (prefixed by its `describe`), all 8 cases. `makeFileItem` and
//! `makeFile` build file items (DEFER with `VirtualizedFile`): the fixtures are
//! `file_like_item` diff items (harness_tests.rs, divergences.psv), whose heights
//! are not a file's; every assertion is a relation between two measurements of
//! the same viewer, as upstream's, and the constants of the rebase are the
//! test-local mirrors upstream keeps. `createClampingRoot` is
//! `harness_tests::create_clamping_root`; the spacer height at each `scrollTo`
//! is what `FakeDom::scroll_writes` records. Test-only.

use std::rc::Rc;

use super::harness_tests::Page;
use super::harness_tests::clamping_page;
use super::harness_tests::diff_item;
use super::harness_tests::file_like_item;
use super::harness_tests::item_target;
use super::harness_tests::line_target;
use super::harness_tests::page;
use super::harness_tests::position_target;
use super::harness_tests::with_defaults;
use super::options::CodeViewOptions;
use crate::constants::DEFAULT_CODE_VIEW_LAYOUT;
use crate::fake_interaction_dom::FakeDom;
use crate::types::layout::CodeViewLayout;
use crate::types::options::DiffStyle;
use crate::types::view::AnnotationSide;
use crate::types::view::CodeViewItem;
use crate::types::view::ScrollAlign;

const ROOT_HEIGHT: f64 = 800.0;

// Test-local mirrors of DEFAULT_SCROLL_REBASE in src/components/CodeView.ts.
// If the source tuning changes, update these mirrors to match.
const SCROLL_REBASE_CONTAINER_HEIGHT: f64 = 12_000_000.0;
const SCROLL_REBASE_TRIGGER_TOP: f64 = 1_000_000.0;
const SCROLL_REBASE_TARGET_TOP: f64 = 2_000_000.0;
// Paged scrollTop above which CodeView rebases the DOM scroll window, derived
// the same way as SCROLL_REBASE_THRESHOLD in src/components/CodeView.ts.
const SCROLL_REBASE_THRESHOLD: f64 = SCROLL_REBASE_CONTAINER_HEIGHT - SCROLL_REBASE_TRIGGER_TOP;
// Logical scroll position slightly past the rebase threshold, so scrolling or
// jumping to it forces a rebase.
const PAST_REBASE_SCROLL_TOP: f64 = SCROLL_REBASE_THRESHOLD + 100_000.0;

// Test-local mirrors of FIREFOX_SCROLL_REBASE in src/components/CodeView.ts.
const FIREFOX_SCROLL_REBASE_CONTAINER_HEIGHT: f64 = 4_194_304.0; // 2 ** 22
const FIREFOX_SCROLL_REBASE_TARGET_TOP: f64 = 1_000_000.0;
const FIREFOX_SCROLL_REBASE_THRESHOLD: f64 = FIREFOX_SCROLL_REBASE_CONTAINER_HEIGHT - 500_000.0;
const FIREFOX_USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:153.0) Gecko/20100101 Firefox/153.0";

fn tall_gap() -> CodeViewOptions<u8, u8> {
    CodeViewOptions {
        layout: Some(CodeViewLayout {
            gap: 1_000_000.0,
            ..DEFAULT_CODE_VIEW_LAYOUT
        }),
        ..CodeViewOptions::default()
    }
}

fn forty_items(prefix: &str) -> Vec<CodeViewItem<u8>> {
    (0..40)
        .map(|index| file_like_item(&format!("{prefix}:{index}"), "line", 1))
        .collect()
}

/// `makeReplacementDiffItem(id, lineCount)`: every line replaced.
fn replacement_diff_item(id: &str, line_count: usize) -> CodeViewItem<u8> {
    let old: Vec<String> = (1..=line_count)
        .map(|index| format!("line {index}"))
        .collect();
    let new: Vec<String> = (1..=line_count)
        .map(|index| format!("replacement {index}"))
        .collect();
    diff_item(id, &old, &new, None)
}

/// `root.firstElementChild`
fn container(page: &Page) -> crate::document::NodeKey {
    page.dom.doc.children(page.root)[0]
}

/// `Number.parseFloat(container.style.height)`, `''` being 0.
fn style_px(page: &Page, node: crate::document::NodeKey, name: &str) -> String {
    page.dom.doc.get_style_property(node, name)
}

fn top_of(page: &Page, id: &str) -> f64 {
    page.view.get_top_for_item(id).unwrap_or(0.0)
}

#[test]
fn code_view_scroll_anchoring_keeps_an_item_anchor_fixed_when_split_to_unified_grows_past_the_old_scroll_range()
 {
    let mut options = CodeViewOptions::default();
    options.diff.base.diff_style = Some(DiffStyle::Split);
    let page = clamping_page(Rc::new(FakeDom::default()), options, ROOT_HEIGHT);
    let anchor = file_like_item("file:anchor", "anchor", 90);
    page.render_items(&[replacement_diff_item("diff:growing", 100), anchor]);

    let split_anchor_top = DEFAULT_CODE_VIEW_LAYOUT.padding_top + top_of(&page, "file:anchor");
    let split_max_scroll_top = page.dom.max_scroll_top();
    assert!(split_max_scroll_top > split_anchor_top);

    page.scroll_and_render(split_anchor_top);

    let mut unified = CodeViewOptions::default();
    unified.diff.base.diff_style = Some(DiffStyle::Unified);
    page.view.set_options(Some(with_defaults(unified)));
    page.view.render(true);

    let unified_anchor_top = DEFAULT_CODE_VIEW_LAYOUT.padding_top + top_of(&page, "file:anchor");
    assert!(unified_anchor_top > split_max_scroll_top);
    assert_eq!(page.scroll_top(), unified_anchor_top);
    page.finish();
}

#[test]
fn code_view_scroll_anchoring_rebases_the_dom_scroll_position_while_preserving_logical_scroll_progress()
 {
    let page = clamping_page(Rc::new(FakeDom::default()), tall_gap(), ROOT_HEIGHT);
    page.render_items(&forty_items("file"));

    assert!(page.view.get_scroll_height() > 20_000_000.0);
    assert!(page.dom.max_scroll_top() < SCROLL_REBASE_CONTAINER_HEIGHT);

    page.scroll_and_render(PAST_REBASE_SCROLL_TOP);

    assert_eq!(page.view.get_scroll_top(), PAST_REBASE_SCROLL_TOP);
    assert_eq!(page.scroll_top(), SCROLL_REBASE_TARGET_TOP);

    // Scrolling the rebased DOM window by a delta must advance the logical
    // scroll position by the same delta.
    let scroll_delta = 1_000_000.0;
    page.scroll_and_render(SCROLL_REBASE_TARGET_TOP + scroll_delta);

    assert_eq!(
        page.view.get_scroll_top(),
        PAST_REBASE_SCROLL_TOP + scroll_delta
    );

    page.view
        .scroll_to(&item_target("file:39", ScrollAlign::Start));
    page.view.render(true);

    let final_file_top = DEFAULT_CODE_VIEW_LAYOUT.padding_top + top_of(&page, "file:39");
    assert!(page.view.get_scroll_top() > final_file_top - ROOT_HEIGHT);
    assert!(page.view.get_scroll_top() <= final_file_top);
    assert!(page.scroll_top() <= page.dom.max_scroll_top());
    assert!(page.rendered_ids().iter().any(|id| id == "file:39"));
    page.finish();
}

#[test]
fn code_view_scroll_anchoring_caps_the_paged_scroll_container_in_firefox() {
    let dom = Rc::new(FakeDom::default());
    {
        let mut info = dom.platform_info.borrow_mut();
        info.platform = Some("MacIntel".to_owned());
        info.user_agent = FIREFOX_USER_AGENT.to_owned();
    }
    let page = clamping_page(dom, tall_gap(), ROOT_HEIGHT);
    page.render_items(&forty_items("file"));

    assert_eq!(
        style_px(&page, container(&page), "height"),
        format!("{FIREFOX_SCROLL_REBASE_CONTAINER_HEIGHT}px")
    );

    let past_firefox_rebase_scroll_top = FIREFOX_SCROLL_REBASE_THRESHOLD + 100_000.0;
    page.scroll_and_render(past_firefox_rebase_scroll_top);

    assert_eq!(page.view.get_scroll_top(), past_firefox_rebase_scroll_top);
    assert_eq!(page.scroll_top(), FIREFOX_SCROLL_REBASE_TARGET_TOP);
    page.finish();
}

#[test]
fn code_view_scroll_anchoring_restores_the_paged_scroll_height_after_clearing_and_reusing_the_viewer()
 {
    let page = clamping_page(Rc::new(FakeDom::default()), tall_gap(), ROOT_HEIGHT);
    page.render_items(&forty_items("first"));

    let container = container(&page);
    assert_eq!(
        style_px(&page, container, "height"),
        format!("{SCROLL_REBASE_CONTAINER_HEIGHT}px")
    );
    let sticky_offset = page.dom.doc.children(container)[0];
    assert_ne!(style_px(&page, sticky_offset, "height"), "");

    page.render_items(&[]);
    assert_eq!(style_px(&page, container, "height"), "0px");
    // An emptied viewer sheds the sticky spacer height along with its items; a
    // stale spacer would keep phantom space in the container.
    assert_eq!(style_px(&page, sticky_offset, "height"), "");

    page.render_items(&forty_items("second"));

    assert!(page.view.get_scroll_height() > 20_000_000.0);
    assert_eq!(
        style_px(&page, container, "height"),
        format!("{SCROLL_REBASE_CONTAINER_HEIGHT}px")
    );
    assert!(page.dom.max_scroll_top() > SCROLL_REBASE_THRESHOLD);
    page.finish();
}

#[test]
fn code_view_scroll_anchoring_keeps_a_rendered_item_anchored_when_reconcile_items_removes_earlier_items()
 {
    let page = page(CodeViewOptions::default(), ROOT_HEIGHT);
    let items: Vec<CodeViewItem<u8>> = (0..60)
        .map(|index| file_like_item(&format!("file:{index}"), "line", 5 + ((index * 7) % 15)))
        .collect();
    let anchor_id = "file:30";
    page.render_items(&items);

    page.view
        .scroll_to(&item_target(anchor_id, ScrollAlign::Start));
    page.view.render(true);
    page.wait_a_frame();

    let anchor_top_before_removal = top_of(&page, anchor_id);
    let scroll_top_before_removal = page.view.get_scroll_top();

    // Remove several earlier items in one call, well outside the current render
    // window but still shifting the index (and therefore the top offset) of
    // every item at or after them, including the anchor. A single removal isn't
    // enough here, even giving the removed item a very different height from
    // its neighbors: the stale scan is off by exactly one slot, which almost
    // always still resolves to the very next real item, and that item's own
    // recomputed shift is identical regardless of what was removed, so the
    // error cancels out. Only a shift big enough to carry the stale, off-by-N
    // read past the old viewport/overscan bounds makes the search fail
    // outright, which is what removing 10 items in one call reliably does.
    page.view.set_items(&items[10..]);
    page.view.render(true);
    page.wait_a_frame();

    let anchor_top_after_removal = top_of(&page, anchor_id);
    let scroll_top_after_removal = page.view.get_scroll_top();

    // The anchor shifted up by the removed items' combined height. A correct
    // anchor keeps it fixed in the viewport, so the scroll position must shift
    // by the same delta. Without capturing the anchor before the reindex, the
    // anchor scan instead finds nothing (it reads stale, not-yet-relaid-out
    // positions through the new index mapping) and scrollTop is left stuck at
    // its old value.
    let anchor_shift = anchor_top_before_removal - anchor_top_after_removal;
    assert!(anchor_shift > 0.0);
    assert_eq!(
        scroll_top_before_removal - scroll_top_after_removal,
        anchor_shift
    );
    page.finish();
}

#[test]
fn code_view_scroll_anchoring_anchors_the_next_item_at_the_top_when_reconcile_items_removes_the_only_visible_anchor()
 {
    let page = page(CodeViewOptions::default(), ROOT_HEIGHT);
    let removed_id = "diff:removed-anchor";
    let next_id = "file:after-anchor";
    let mut items = vec![
        file_like_item("file:before-anchor", "line", 120),
        replacement_diff_item(removed_id, 240),
        file_like_item(next_id, "line", 80),
    ];
    items.extend((0..8).map(|index| file_like_item(&format!("file:trailing-{index}"), "line", 80)));
    page.render_items(&items);

    page.view.scroll_to(&line_target(
        removed_id,
        80,
        Some(AnnotationSide::Additions),
        ScrollAlign::Start,
    ));
    page.view.render(true);
    page.wait_a_frame();

    // Keep the successor outside the old render window so reconciliation has to
    // anchor its item header at the top instead of finding a visible survivor.
    assert!(!page.rendered_ids().iter().any(|id| id == next_id));

    let remaining: Vec<CodeViewItem<u8>> = items
        .iter()
        .filter(|item| !matches!(item, CodeViewItem::Diff(diff) if diff.id == removed_id))
        .cloned()
        .collect();
    page.view.set_items(&remaining);
    page.view.render(true);
    page.wait_a_frame();

    let next_top = page
        .view
        .get_top_for_item(next_id)
        .expect("the promoted successor");
    assert_eq!(next_top - page.view.get_scroll_top(), 0.0);
    page.finish();
}

#[test]
fn code_view_scroll_anchoring_skips_a_removed_line_anchor_for_a_visible_surviving_item() {
    let page = page(CodeViewOptions::default(), ROOT_HEIGHT);
    let removed_id = "diff:visible-removed-anchor";
    let next_id = "file:visible-after-anchor";
    let items = vec![
        file_like_item("file:visible-before-anchor", "line", 100),
        replacement_diff_item(removed_id, 12),
        file_like_item(next_id, "line", 40),
        file_like_item("file:visible-trailing", "line", 100),
    ];
    page.render_items(&items);

    page.view.scroll_to(&line_target(
        removed_id,
        4,
        Some(AnnotationSide::Additions),
        ScrollAlign::Start,
    ));
    page.view.render(true);
    page.wait_a_frame();

    assert!(page.rendered_ids().iter().any(|id| id == next_id));
    let next_top_before_removal = page
        .view
        .get_top_for_item(next_id)
        .expect("the visible successor");
    let next_viewport_offset =
        DEFAULT_CODE_VIEW_LAYOUT.padding_top + next_top_before_removal - page.view.get_scroll_top();

    let remaining: Vec<CodeViewItem<u8>> = items
        .iter()
        .filter(|item| !matches!(item, CodeViewItem::Diff(diff) if diff.id == removed_id))
        .cloned()
        .collect();
    page.view.set_items(&remaining);
    page.view.render(true);
    page.wait_a_frame();

    let next_top_after_removal = page
        .view
        .get_top_for_item(next_id)
        .expect("the surviving item");
    assert_eq!(
        DEFAULT_CODE_VIEW_LAYOUT.padding_top + next_top_after_removal - page.view.get_scroll_top(),
        next_viewport_offset
    );
    page.finish();
}

#[test]
fn code_view_scroll_anchoring_moves_the_physical_spacer_before_applying_a_programmatic_rebase_jump()
{
    let page = clamping_page(Rc::new(FakeDom::default()), tall_gap(), ROOT_HEIGHT);
    page.render_items(&forty_items("file"));

    page.view
        .scroll_to(&position_target(PAST_REBASE_SCROLL_TOP));
    page.view.render(true);

    let rebase_write = page
        .dom
        .scroll_writes
        .borrow()
        .iter()
        .find(|(top, _)| *top == SCROLL_REBASE_TARGET_TOP)
        .copied();
    let (_, spacer_height) = rebase_write.expect("a write at the rebase target");
    // The spacer height tracks where the rendered window starts, which must
    // already sit near the rebase target when the scroll write happens.
    let spacer_tolerance = 100_000.0;
    assert!(spacer_height > SCROLL_REBASE_TARGET_TOP - spacer_tolerance);
    assert!(spacer_height < SCROLL_REBASE_TARGET_TOP + spacer_tolerance);
    page.finish();
}
