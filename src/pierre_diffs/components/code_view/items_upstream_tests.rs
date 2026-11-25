//! What the item model pins on its own, without a render pass (C2): the
//! upstream cases it came from (`test/CodeView.removeItem.test.ts`,
//! `updateItemId`, `collapsed`) are whole in `item_cases_upstream_tests.rs`
//! since C5. Here: the retargeting of a pending scroll target and layout
//! anchor by a rename, the duplicate-id and deferred-file-item arms, and the
//! append fast path. Test-only.

use std::rc::Rc;

use super::CodeView;
use super::options::CodeViewOptions;
use super::state::ItemAnchor;
use super::state::PendingAlign;
use super::state::PendingItemTarget;
use super::state::PendingScrollTarget;
use super::state::ScrollAnchor;
use crate::components::virtualized_file_diff::advanced::AdvancedVirtualizer;
use crate::components::virtualized_file_diff::layout_upstream_tests::parse_files;
use crate::components::virtualized_file_diff::layout_upstream_tests::plain_file;
use crate::components::virtualizer::InstanceRef;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::CodeViewFileItem;
use crate::types::view::CodeViewItem;
use crate::types::view::CodeViewItemScrollTarget;

type View = CodeView<FakeDom, u8, u8>;

/// A viewer over `FakeDom`, no root and no render pass.
struct Harness {
    dom: Rc<FakeDom>,
    view: Rc<View>,
}

fn harness() -> Harness {
    let dom = Rc::new(FakeDom::default());
    let view = CodeView::new(
        Rc::clone(&dom),
        ResizeObserverHub::new(Rc::clone(&dom)),
        UniversalRenderingManager::new(Rc::clone(&dom)),
        Rc::new(PlainTextHighlighter),
        CodeViewOptions::default(),
    );
    Harness { dom, view }
}

/// `makeDiffItem` of `CodeView.collapsed.test.ts`, with a version.
fn diff_item(id: &str, collapsed: Option<bool>, version: Option<usize>) -> CodeViewItem<u8> {
    CodeViewItem::Diff(CodeViewDiffItem {
        id: id.to_owned(),
        file_diff: parse_files(
            plain_file("src/example.txt", "one\ntwo\nthree\n"),
            plain_file("src/example.txt", "one\ntwo changed\nthree\n"),
        ),
        annotations: None,
        version,
        collapsed,
        edit: None,
    })
}

fn height_of(view: &View, id: &str) -> f64 {
    view.item_by_id(id)
        .expect("the item exists")
        .borrow()
        .height
}

// ---- the model half of what the whole cases pin ---------------------------------

/// Pins what `does not mutate a pending scroll target passed by the caller`
/// needs of the model (the case itself drives `scrollTo` and a render, C3 and
/// C5): the rename retargets the pending target and the pending layout anchor
/// to the new id, as new values.
#[test]
fn code_view_item_id_updates_retargets_a_pending_scroll_target_and_layout_anchor() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("file:old", None, None)]);
    let target = PendingScrollTarget::Item(PendingItemTarget {
        target: CodeViewItemScrollTarget {
            id: "file:old".to_owned(),
            align: None,
            offset: None,
            behavior: None,
        },
        align: Some(PendingAlign::Center),
    });
    {
        let mut state = view.state.borrow_mut();
        state.pending_scroll_target = Some(target.clone());
        state.pending_layout_anchor = Some(ScrollAnchor::Item(ItemAnchor {
            id: "file:old".to_owned(),
            viewport_offset: 4.0,
        }));
    }

    assert!(view.update_item_id("file:old", "file:new"));

    let state = view.state.borrow();
    assert_eq!(
        state
            .pending_scroll_target
            .as_ref()
            .and_then(PendingScrollTarget::id),
        Some("file:new")
    );
    assert_eq!(
        state.pending_layout_anchor.as_ref().map(ScrollAnchor::id),
        Some("file:new")
    );
    // The caller's target is untouched.
    assert_eq!(target.id(), Some("file:old"));
}

// ---- arms no upstream case reaches ----------------------------------------------

#[test]
#[should_panic(expected = "CodeView.addItem: duplicate id \"a\"")]
fn adding_a_duplicate_id_throws() {
    let Harness { view, .. } = harness();
    view.add_items(&[diff_item("a", None, None), diff_item("a", None, None)]);
}

#[test]
#[should_panic(expected = "CodeView.setItems: duplicate id \"a\"")]
fn reconciling_a_duplicate_id_throws() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("a", None, None), diff_item("b", None, None)]);
    // Not an append of the current prefix, so it reconciles.
    view.set_items(&[
        diff_item("b", None, None),
        diff_item("a", None, None),
        diff_item("a", None, None),
    ]);
}

#[test]
#[should_panic(expected = "file items: VirtualizedFile is DEFER")]
fn a_file_item_is_the_deferred_branch() {
    let Harness { view, .. } = harness();
    view.add_item(&CodeViewItem::File(CodeViewFileItem {
        id: "file".to_owned(),
        file: plain_file("a.txt", "a\n"),
        annotations: None,
        version: None,
        collapsed: None,
        edit: None,
    }));
}

#[test]
fn appending_to_a_list_keeps_the_existing_records_and_instances() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("a", None, None), diff_item("b", None, None)]);
    let first = Rc::clone(view.item_by_id("a").expect("the item").borrow().instance());

    view.set_items(&[
        diff_item("a", None, None),
        diff_item("b", None, None),
        diff_item("c", None, None),
    ]);

    assert!(Rc::ptr_eq(
        &first,
        view.item_by_id("a").expect("the item").borrow().instance()
    ));
    let layout = view.get_layout();
    let (a, b, c) = (
        height_of(&view, "a"),
        height_of(&view, "b"),
        height_of(&view, "c"),
    );
    assert_eq!(view.get_scroll_height(), a + b + c + 2.0 * layout.gap);
    assert_eq!(
        view.item_by_id("c").expect("the item").borrow().top,
        a + b + 2.0 * layout.gap
    );
}

#[test]
fn an_instance_that_is_not_registered_cannot_change() {
    let Harness { view, .. } = harness();
    let Harness { view: other, .. } = harness();
    other.set_items(&[diff_item("a", None, None)]);
    let foreign = Rc::clone(other.item_by_id("a").expect("the item").borrow().instance());
    let foreign: InstanceRef = foreign;

    let message = crate::components::file_diff_hydration_upstream_tests::panic_message(|| {
        view.instance_changed(&foreign, true);
    });

    assert!(message.contains("An instance has changed that is not registered"));
}

#[test]
fn the_top_of_an_item_adds_the_padding_and_the_header_host() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("a", None, None), diff_item("b", None, None)]);
    let layout = view.get_layout();
    let local_top = view.item_by_id("b").expect("the item").borrow().top;

    assert_eq!(
        view.get_top_for_item("b"),
        Some(local_top + layout.padding_top)
    );
    view.state.borrow_mut().header.height = 30.0;
    assert_eq!(
        view.get_top_for_item("b"),
        Some(local_top + layout.padding_top + 30.0)
    );
    assert_eq!(view.get_top_for_item("zzz"), None);
}

#[test]
fn skipping_the_render_for_an_append_needs_a_rendered_range_below_which_it_lands() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("a", None, None)]);
    // Nothing has rendered: always render.
    assert!(!view.can_skip_render_for_append(1e9));

    {
        let mut state = view.state.borrow_mut();
        state.render_state.first_index = 0;
        state.window_specs.bottom = 500.0;
    }
    assert!(view.can_skip_render_for_append(501.0));
    assert!(!view.can_skip_render_for_append(500.0));

    view.state.borrow_mut().layout_dirty_index = Some(0);
    assert!(!view.can_skip_render_for_append(501.0));
}

#[test]
fn visible_indexes_bound_the_items_that_intersect_a_window() {
    let Harness { view, .. } = harness();
    view.set_items(
        &(0..5)
            .map(|index| diff_item(&format!("{index}"), None, None))
            .collect::<Vec<_>>(),
    );
    let tops: Vec<(f64, f64)> = view
        .state
        .borrow()
        .items
        .iter()
        .map(|item| (item.borrow().top, item.borrow().height))
        .collect();

    // The first item whose bottom is below the window top, the last whose top is
    // at or above the window bottom.
    assert_eq!(view.find_first_visible_index(-1.0), 0);
    assert_eq!(
        view.find_first_visible_index(tops[2].0 + tops[2].1 - 0.5),
        2
    );
    assert_eq!(view.find_first_visible_index(tops[4].0 + tops[4].1), 5);
    assert_eq!(view.find_last_visible_index(-1.0), None);
    assert_eq!(view.find_last_visible_index(tops[3].0), Some(3));
    assert_eq!(view.find_last_visible_index(1e9), Some(4));
}

#[test]
fn the_device_pixel_ratio_and_reduced_motion_are_host_reads() {
    use super::CodeViewHost;
    let Harness { dom, .. } = harness();
    assert_eq!(dom.device_pixel_ratio(), 1.0);
    dom.device_pixel_ratio.set(1.5);
    assert_eq!(dom.device_pixel_ratio(), 1.5);
    assert!(!dom.prefers_reduced_motion());
    dom.prefers_reduced_motion.set(true);
    assert!(dom.prefers_reduced_motion());
}

#[test]
fn local_top_is_the_items_top() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("a", None, None), diff_item("b", None, None)]);
    let instance = Rc::clone(view.item_by_id("b").expect("the item").borrow().instance());
    let instance: InstanceRef = instance;
    assert_eq!(
        view.get_local_top_for_instance(&instance),
        view.item_by_id("b").expect("the item").borrow().top
    );
}
