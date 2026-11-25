// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/CodeView.elementPooling.test.ts
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; file items become diff items (divergences.psv).

//! Upstream's `test/CodeView.elementPooling.test.ts` (C5), ported with its
//! names (prefixed by its `describe`): 5 of its 6 cases. The sixth, `waits for
//! managed slot children to clear before reusing a shell`, drives the React
//! `CodeViewCoordinator` and the component-managed mode, which are not ported
//! (the Dioxus view is not component-managed, closure notes Q5; rules.psv).
//!
//! `makeFileItem` builds file items (DEFER with `VirtualizedFile`): the fixtures
//! are `file_like_item` diff items (harness_tests.rs, divergences.psv). The
//! counts of the shell's assets are exact as upstream's; the scroll jump of
//! `recycled instances do not steal sprite SVGs from live shells` is derived
//! from the item tops, because the fixtures' heights are not a file's. `waitFor`
//! and `waitForShellCounts` poll an asynchronous highlighter; the port renders
//! synchronously, so each is the one assertion. Test-only.

use std::rc::Rc;

use super::harness_tests::Page;
use super::harness_tests::file_like_item;
use super::harness_tests::page;
use super::harness_tests::shadow_count;
use super::harness_tests::small_diff_item;
use super::harness_tests::with_defaults;
use super::options::CodeViewCallbacks;
use super::options::CodeViewDiffItemContext;
use super::options::CodeViewOptions;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::js_collections::PtrEq;
use crate::types::options::ThemeTypes;
use crate::types::patch::FileDiffMetadata;

const ROOT_HEIGHT: f64 = 120.0;

/// A `renderHeaderPrefix` callback with its item context.
type HeaderPrefix = dyn Fn(&FileDiffMetadata, &CodeViewDiffItemContext<u8>) -> Option<u8>;

/// `getShellCounts(element)`: `(pre, svg[data-icon-sprite], style[data-theme-css],
/// style[data-unsafe-css])`.
fn shell_counts(dom: &FakeDom, element: NodeKey) -> (usize, usize, usize, usize) {
    (
        shadow_count(dom, element, "pre", None),
        shadow_count(dom, element, "svg", Some("data-icon-sprite")),
        shadow_count(dom, element, "style", Some("data-theme-css")),
        shadow_count(dom, element, "style", Some("data-unsafe-css")),
    )
}

/// `getShadowText(element)`
fn shadow_text(dom: &FakeDom, element: NodeKey) -> String {
    dom.doc
        .shadow_root(element)
        .map_or_else(String::new, |shadow| dom.doc.text_content(shadow))
}

/// `getHeaderCount(element)`
fn header_count(dom: &FakeDom, element: NodeKey) -> usize {
    shadow_count(dom, element, "div", Some("data-diffs-header"))
}

fn header_prefix_count(dom: &FakeDom, element: NodeKey) -> usize {
    dom.doc
        .query_by_attribute(element, "slot", Some("header-prefix"))
        .len()
}

/// `viewer.getRenderedItems()[0].element`
fn shell_of_first(page: &Page) -> NodeKey {
    page.rendered_items()[0]
        .borrow()
        .element
        .expect("a rendered item holds a shell")
}

#[test]
fn code_view_element_pooling_reuses_sanitized_item_shells_without_duplicating_shared_assets() {
    let mut options = CodeViewOptions::default();
    options.diff.base.base.disable_file_header = Some(true);
    options.diff.base.base.unsafe_css = Some(":host { --pooled-shell: 1; }".to_owned());
    let page = page(options, ROOT_HEIGHT);
    page.render_items(&[
        file_like_item("file:first", "first pooled content", 100),
        file_like_item("file:second", "second pooled content", 100),
    ]);

    assert_eq!(page.rendered_ids(), ["file:first"]);
    let first_element = shell_of_first(&page);
    assert_eq!(shell_counts(&page.dom, first_element), (1, 1, 1, 1));
    assert!(shadow_text(&page.dom, first_element).contains("first pooled content"));

    page.scroll_and_render(2_400.0);
    page.wait_a_frame();

    assert_eq!(page.rendered_ids(), ["file:second"]);
    let second_element = shell_of_first(&page);
    assert_eq!(second_element, first_element);
    assert_eq!(shell_counts(&page.dom, second_element), (1, 1, 1, 1));
    let text = shadow_text(&page.dom, second_element);
    assert!(text.contains("second pooled content"));
    assert!(!text.contains("first pooled content"));
    page.finish();
}

// Header slot nodes produced by options callbacks (renderHeaderPrefix and
// friends) are light-DOM children of the host element, owned by the
// File/FileDiff instance rather than the host application. Releasing a row
// must remove them: a shell with leftover light-DOM children never qualifies
// as clean, which would exclude it from pooled reuse.
#[test]
fn code_view_element_pooling_reuses_shells_for_rows_that_render_header_slot_content() {
    let prefix: Rc<HeaderPrefix> = Rc::new(|_, _| Some(1));
    let page = page(
        CodeViewOptions {
            callbacks: CodeViewCallbacks {
                render_header_prefix: Some(PtrEq::new(prefix)),
                ..CodeViewCallbacks::default()
            },
            ..CodeViewOptions::default()
        },
        ROOT_HEIGHT,
    );
    page.render_items(&[
        file_like_item("file:first", "first slotted content", 100),
        file_like_item("file:second", "second slotted content", 100),
    ]);

    assert_eq!(page.rendered_ids(), ["file:first"]);
    let first_element = shell_of_first(&page);
    assert_eq!(header_prefix_count(&page.dom, first_element), 1);

    page.scroll_and_render(2_400.0);
    page.wait_a_frame();

    assert_eq!(page.rendered_ids(), ["file:second"]);
    assert_eq!(page.element_of("file:second"), Some(first_element));
    assert_eq!(header_prefix_count(&page.dom, first_element), 1);
    page.finish();
}

#[test]
fn code_view_element_pooling_clears_pooled_shells_when_shared_css_options_change() {
    let themed = |theme_type| {
        let mut options = CodeViewOptions::default();
        options.diff.base.base.disable_file_header = Some(true);
        options.diff.base.base.theme_type = Some(theme_type);
        options
    };
    let page = page(themed(ThemeTypes::Light), 1000.0);
    page.render_items(&[
        file_like_item("file:first", "first content", 5),
        file_like_item("file:second", "second content", 5),
    ]);

    let pooled_candidates: Vec<NodeKey> = page
        .rendered_items()
        .iter()
        .map(|item| {
            item.borrow()
                .element
                .expect("a rendered item holds a shell")
        })
        .collect();
    assert_eq!(pooled_candidates.len(), 2);

    page.view.set_items(&[]);
    page.view
        .set_options(Some(with_defaults(themed(ThemeTypes::Dark))));
    page.render_items(&[file_like_item("file:third", "third content", 5)]);

    let next_element = page
        .rendered_items()
        .first()
        .and_then(|item| item.borrow().element)
        .expect("the third item is rendered");
    assert!(!pooled_candidates.contains(&next_element));
    page.finish();
}

// Toggling disableFileHeader does not clear the element pool. A row remounted
// from a pooled shell must not display a header carried over from the shell's
// previous occupant.
#[test]
fn code_view_element_pooling_drops_stale_pooled_headers_when_disable_file_header_is_enabled() {
    let page = page(CodeViewOptions::default(), 1000.0);
    page.render_items(&[
        small_diff_item("diff:first", "first.ts"),
        file_like_item("file:second", "second pooled header", 5),
    ]);

    let pooled_candidates: Vec<NodeKey> = page
        .rendered_items()
        .iter()
        .map(|item| {
            item.borrow()
                .element
                .expect("a rendered item holds a shell")
        })
        .collect();
    assert_eq!(pooled_candidates.len(), 2);
    for element in &pooled_candidates {
        assert_eq!(header_count(&page.dom, *element), 1);
    }

    // Release both rows into the pool, then disable headers: this option
    // change intentionally keeps the pool.
    page.view.set_items(&[]);
    let mut options = CodeViewOptions::default();
    options.diff.base.base.disable_file_header = Some(true);
    page.view.set_options(Some(with_defaults(options)));
    page.render_items(&[
        small_diff_item("diff:third", "third.ts"),
        file_like_item("file:fourth", "fourth pooled header", 5),
    ]);

    let remounted_items = page.rendered_items();
    assert_eq!(page.rendered_ids(), ["diff:third", "file:fourth"]);
    for item in remounted_items {
        let element = item
            .borrow()
            .element
            .expect("a rendered item holds a shell");
        // The rows must reuse pooled shells, otherwise this test would not
        // exercise pooled reuse at all.
        assert!(pooled_candidates.contains(&element));
        assert_eq!(header_count(&page.dom, element), 0);
    }
    page.finish();
}

// A recycled File instance must drop its sprite SVG ref. Pooled shells keep
// their sprite for the next occupant to adopt; a stale ref makes
// adoptReusableShellElements skip the new shell's own sprite, and
// ensureSpriteSVG then reparents the stale sprite out of whichever live item's
// shadow root currently holds it, blanking that item's icons.
#[test]
fn code_view_element_pooling_recycled_instances_do_not_steal_sprite_svgs_from_live_shells() {
    let page = page(CodeViewOptions::default(), ROOT_HEIGHT);
    // One is tall, the 10-line two is short enough to sit fully inside every
    // render window below, so its renders early-return once mounted (like any
    // live fully-visible item) and it never re-adopts its sprite. Three pads
    // the scroll range.
    page.render_items(&[
        file_like_item("file:one", "first sprite content", 100),
        file_like_item("file:two", "second sprite content", 10),
        file_like_item("file:three", "third sprite content", 100),
    ]);
    let sprite_count =
        |element: NodeKey| shadow_count(&page.dom, element, "svg", Some("data-icon-sprite"));

    assert_eq!(page.rendered_ids(), ["file:one"]);
    let first_element = shell_of_first(&page);
    assert_eq!(sprite_count(first_element), 1);

    // Commit the initial height correction before testing the large-jump
    // recycle path below.
    page.view.render(true);
    page.wait_a_frame();

    // Jump past one: the fit-perfectly pass releases one (its shell and sprite
    // go to the pool, and the instance is recycled) and mounts two into that
    // shell, adopting the pooled sprite. The follow-up fill pass then remounts
    // one into a brand-new empty shell. If one's recycled instance kept its
    // sprite ref, ensureSpriteSVG reparents that sprite between the two live
    // shadow roots instead of creating a new one, and whichever shell rendered
    // first is left without any sprite. Upstream's 2_116 is 48px below the top
    // of two, which puts one just outside the fit-perfectly window: the same
    // offset from this fixture's top of two.
    let top_of_two = page
        .view
        .get_top_for_item("file:two")
        .expect("two is laid out");
    page.scroll_and_render(top_of_two + 48.0);
    for _ in 0..10 {
        if page.rendered_items().len() == 3 {
            break;
        }
        page.wait_a_frame();
    }
    page.wait_a_frame();

    let rendered = page.rendered_items();
    assert_eq!(page.rendered_ids(), ["file:one", "file:two", "file:three"]);
    let elements: Vec<NodeKey> = rendered
        .iter()
        .map(|item| {
            item.borrow()
                .element
                .expect("a rendered item holds a shell")
        })
        .collect();
    assert_eq!(elements[1], first_element);
    assert_ne!(elements[0], first_element);
    for element in elements {
        assert_eq!(sprite_count(element), 1);
    }
    page.finish();
}
