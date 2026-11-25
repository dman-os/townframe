// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/CodeView.headerFooter.test.ts
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; file items become diff items (divergences.psv).

//! Upstream's `test/CodeView.headerFooter.test.ts` (C5), ported with its names
//! (prefixed by its `describe`), all 10 cases. `makeFileItem` builds a file
//! item, which is DEFER with `VirtualizedFile`: the fixtures are `file_like_item`
//! diff items (harness_tests.rs, divergences.psv), so no case asserts an absolute
//! height, only the relations upstream asserts between two measurements of the
//! same viewer. `document.createElement('div')` as the host's content is a slot
//! callback answering `Some(content)`; the synchronous measure of
//! `measureMountedHosts` reads the host's `getBoundingClientRect` height, which
//! `stubRectHeight` scripts as `FakeDom::heights`. Test-only.

use std::rc::Rc;

use super::harness_tests::Page;
use super::harness_tests::file_like_item;
use super::harness_tests::footer_element;
use super::harness_tests::header_element;
use super::harness_tests::item_target;
use super::harness_tests::page;
use super::harness_tests::position_target;
use super::harness_tests::with_defaults;
use super::options::CodeViewOptions;
use super::options::RenderCodeViewSlotCallback;
use crate::constants::CODE_VIEW_FOOTER_ATTRIBUTE;
use crate::constants::CODE_VIEW_HEADER_ATTRIBUTE;
use crate::constants::DEFAULT_CODE_VIEW_LAYOUT;
use crate::document::NodeKey;
use crate::js_collections::PtrEq;
use crate::types::view::CodeViewItem;
use crate::types::view::ScrollAlign;

const ROOT_HEIGHT: f64 = 800.0;

fn make_items() -> Vec<CodeViewItem<u8>> {
    (0..12)
        .map(|index| file_like_item(&format!("file:{index}"), "line", 20))
        .collect()
}

/// `() => document.createElement('div')`: a callback of its own each time, as
/// the arrow function of each upstream call is.
fn slot(content: u8) -> RenderCodeViewSlotCallback<u8> {
    let callback: Rc<dyn Fn() -> Option<u8>> = Rc::new(move || Some(content));
    PtrEq::new(callback)
}

fn header_option() -> CodeViewOptions<u8, u8> {
    CodeViewOptions {
        render_code_view_header: Some(slot(1)),
        ..CodeViewOptions::default()
    }
}

fn header_and_footer_option() -> CodeViewOptions<u8, u8> {
    CodeViewOptions {
        render_code_view_header: Some(slot(1)),
        render_code_view_footer: Some(slot(2)),
        ..CodeViewOptions::default()
    }
}

fn must_get_host(host: Option<NodeKey>) -> NodeKey {
    host.expect("expected header/footer host element to be mounted")
}

/// `resizeHost(viewer, host, blockSize)`: drive a host's height through the
/// resize handler, the path the real `ResizeObserver` would trigger, then flush
/// the re-anchor and the queued render.
fn resize_host(page: &Page, host: Option<NodeKey>, block_size: f64) {
    let host = host.expect("resizeHost: host element is not mounted");
    page.resize(host, block_size);
    page.view.render(true);
}

fn top_of(page: &Page, id: &str) -> f64 {
    page.view.get_top_for_item(id).unwrap_or(0.0)
}

fn has_attribute_below(page: &Page, attribute: &str) -> bool {
    !page
        .dom
        .doc
        .query_by_attribute(page.root, attribute, None)
        .is_empty()
}

#[test]
fn code_view_header_footer_mounts_header_footer_hosts_as_flanking_siblings_of_the_container() {
    let page = page(header_and_footer_option(), ROOT_HEIGHT);
    page.render_items(&make_items());

    let header = must_get_host(header_element(&page.view));
    let footer = must_get_host(footer_element(&page.view));
    let doc = &page.dom.doc;
    // root children are [header, container, footer].
    let children = doc.children(page.root);
    assert_eq!(children.len(), 3);
    assert_eq!(children.first(), Some(&header));
    assert_eq!(children.last(), Some(&footer));
    assert_eq!(
        doc.get_attribute(header, CODE_VIEW_HEADER_ATTRIBUTE)
            .as_deref(),
        Some("")
    );
    assert_eq!(
        doc.get_attribute(footer, CODE_VIEW_FOOTER_ATTRIBUTE)
            .as_deref(),
        Some("")
    );
    // Block formatting context so caller margins can't collapse out.
    assert_eq!(doc.get_style_property(header, "display"), "flow-root");
    page.finish();
}

#[test]
fn code_view_header_footer_header_height_offsets_an_item_absolute_top_by_exactly_the_header_height()
{
    let page = page(header_option(), ROOT_HEIGHT);
    page.render_items(&make_items());

    let before = top_of(&page, "file:5");
    resize_host(&page, header_element(&page.view), 140.0);
    let after = top_of(&page, "file:5");

    assert_eq!(after - before, 140.0);
    page.finish();
}

#[test]
fn code_view_header_footer_header_footer_heights_extend_the_max_scroll_range() {
    let page = page(header_and_footer_option(), ROOT_HEIGHT);
    page.render_items(&make_items());

    resize_host(&page, header_element(&page.view), 150.0);
    resize_host(&page, footer_element(&page.view), 250.0);

    page.view.scroll_to(&position_target(1e9));
    page.view.render(true);

    let expected_max = (DEFAULT_CODE_VIEW_LAYOUT.padding_top
        + 150.0
        + page.view.get_scroll_height()
        + 250.0
        + DEFAULT_CODE_VIEW_LAYOUT.padding_bottom
        - ROOT_HEIGHT)
        .max(0.0);
    assert_eq!(page.view.get_scroll_top(), expected_max);
    page.finish();
}

#[test]
fn code_view_header_footer_scrolls_to_an_item_at_its_header_inclusive_absolute_top() {
    let page = page(header_option(), ROOT_HEIGHT);
    page.render_items(&make_items());
    resize_host(&page, header_element(&page.view), 120.0);

    page.view
        .scroll_to(&item_target("file:6", ScrollAlign::Start));
    page.view.render(true);

    // align 'start' lands the item's absolute top (which includes the header
    // offset) at the viewport top.
    let target_top = top_of(&page, "file:6");
    assert_eq!(page.view.get_scroll_top(), target_top);
    assert!(page.rendered_ids().iter().any(|id| id == "file:6"));
    page.finish();
}

#[test]
fn code_view_header_footer_reveals_a_header_that_grows_while_scrolled_to_the_top_no_anti_jump() {
    let page = page(header_option(), ROOT_HEIGHT);
    page.render_items(&make_items());
    assert_eq!(page.view.get_scroll_top(), 0.0);

    resize_host(&page, header_element(&page.view), 300.0);

    // At the top there is nothing above the viewport to keep stable, so the
    // header reveals (content shifts down) instead of being anchored away.
    assert_eq!(page.view.get_scroll_top(), 0.0);
    page.finish();
}

#[test]
fn code_view_header_footer_re_anchors_content_when_a_header_grows_while_scrolled_into_the_list() {
    let page = page(header_option(), ROOT_HEIGHT);
    page.render_items(&make_items());

    page.scroll_and_render(300.0);
    assert_eq!(page.view.get_scroll_top(), 300.0);

    resize_host(&page, header_element(&page.view), 120.0);

    // The anchored item's viewport offset is preserved: the whole list shifted
    // down by the header height, so scrollTop grows by the same amount.
    assert_eq!(page.view.get_scroll_top(), 420.0);
    page.finish();
}

#[test]
fn code_view_header_footer_measures_the_header_height_synchronously_on_mount() {
    let page = page(header_option(), ROOT_HEIGHT);
    page.render_items(&make_items());

    let before = top_of(&page, "file:2");
    // Stub the host rect, then swap the callback to force a fresh inline
    // measure via measureMountedHosts (getBoundingClientRect), not the RO.
    let host = must_get_host(header_element(&page.view));
    page.dom.heights.borrow_mut().insert(host, 90.0);
    page.view.set_options(Some(with_defaults(CodeViewOptions {
        render_code_view_header: Some(slot(3)),
        ..CodeViewOptions::default()
    })));
    page.view.render(true);

    let after = top_of(&page, "file:2");
    assert_eq!(after - before, 90.0);
    page.finish();
}

#[test]
fn code_view_header_footer_removing_the_header_callback_tears_down_the_host_and_zeroes_the_offset()
{
    let page = page(header_option(), ROOT_HEIGHT);
    page.render_items(&make_items());
    resize_host(&page, header_element(&page.view), 150.0);

    let with_header = top_of(&page, "file:3");

    page.view
        .set_options(Some(with_defaults(CodeViewOptions::default())));
    page.view.render(true);

    assert_eq!(header_element(&page.view), None);
    assert!(!has_attribute_below(&page, CODE_VIEW_HEADER_ATTRIBUTE));

    let without_header = top_of(&page, "file:3");
    assert_eq!(with_header - without_header, 150.0);
    page.finish();
}

#[test]
fn code_view_header_footer_mounts_a_header_on_an_empty_code_view_and_stays_within_a_valid_scroll_range()
 {
    let page = page(header_option(), ROOT_HEIGHT);
    page.render_items(&[]);

    assert!(header_element(&page.view).is_some());

    resize_host(&page, header_element(&page.view), 2000.0);
    page.view.scroll_to(&position_target(1e9));
    page.view.render(true);

    let expected_max =
        (DEFAULT_CODE_VIEW_LAYOUT.padding_top + 2000.0 + DEFAULT_CODE_VIEW_LAYOUT.padding_bottom
            - ROOT_HEIGHT)
            .max(0.0);
    assert_eq!(page.view.get_scroll_top(), expected_max);
    page.finish();
}

#[test]
fn code_view_header_footer_clean_up_removes_the_header_footer_hosts() {
    let page = page(header_and_footer_option(), ROOT_HEIGHT);
    page.render_items(&make_items());
    assert!(header_element(&page.view).is_some());

    page.view.clean_up();

    assert_eq!(header_element(&page.view), None);
    assert_eq!(footer_element(&page.view), None);
    assert!(!has_attribute_below(&page, CODE_VIEW_HEADER_ATTRIBUTE));
    assert!(!has_attribute_below(&page, CODE_VIEW_FOOTER_ATTRIBUTE));
    page.wait_a_frame();
}
