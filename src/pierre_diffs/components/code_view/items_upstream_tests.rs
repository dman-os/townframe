//! Upstream's `CodeView` cases that need only the item model (C2), ported with
//! their names (prefixed by their `describe`):
//! * `test/CodeView.removeItem.test.ts`
//! * `test/CodeView.updateItemId.test.ts`, its first case
//! * `test/CodeView.collapsed.test.ts`, the layout half of the cases
//!
//! Every upstream case runs `setup(root)` and `renderItems` (a render pass and
//! the rendered items), which is C3 to C5; what these tests keep is the part
//! of each case that is the item model: ids and order, the selection and its
//! `onSelectedLinesChange` notifications, heights and `scrollHeight` after the
//! layout is recomputed (`recompute`, the layout half of
//! `computeRenderRangeAndEmit`). The rendered halves (`getRenderedItems`, the
//! `<pre>` of a collapsed item, `root.scrollTop`) come back in C5. The upstream
//! cases build file items (`makeFileItem`, `makeFile`), which are DEFER with
//! `VirtualizedFile`: the fixtures are diff items (parity/divergences.psv), so
//! every height differs from upstream's; the assertions are the relations
//! upstream asserts (shorter than expanded, N times one collapsed height), not
//! numbers. `console.error`'s message is not asserted: the port logs nothing.
//! Below them, tests of arms no upstream case reaches. Test-only.

use std::cell::RefCell;
use std::rc::Rc;

use super::CodeView;
use super::options::CodeViewOptions;
use super::state::CodeViewLineSelection;
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
use crate::managers::interaction_manager::SelectionWriteOptions;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::CodeViewFileItem;
use crate::types::view::CodeViewItem;
use crate::types::view::CodeViewItemScrollTarget;
use crate::types::view::SelectedLineRange;

type View = CodeView<FakeDom, u8, u8>;

/// A viewer over `FakeDom` and the selection changes its `onSelectedLinesChange`
/// reported.
struct Harness {
    dom: Rc<FakeDom>,
    view: Rc<View>,
    changes: Rc<RefCell<Vec<Option<CodeViewLineSelection>>>>,
}

fn harness() -> Harness {
    let changes: Rc<RefCell<Vec<Option<CodeViewLineSelection>>>> = Rc::default();
    let recorded = Rc::clone(&changes);
    let options = CodeViewOptions {
        on_selected_lines_change: Some(Rc::new(move |selection| {
            recorded.borrow_mut().push(selection.cloned());
        })),
        ..CodeViewOptions::default()
    };
    let dom = Rc::new(FakeDom::default());
    let view = CodeView::new(
        Rc::clone(&dom),
        ResizeObserverHub::new(Rc::clone(&dom)),
        UniversalRenderingManager::new(Rc::clone(&dom)),
        Rc::new(PlainTextHighlighter),
        options,
    );
    Harness { dom, view, changes }
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

fn ids(view: &View) -> Vec<String> {
    view.state
        .borrow()
        .items
        .iter()
        .map(|item| item.borrow().id().to_owned())
        .collect()
}

fn height_of(view: &View, id: &str) -> f64 {
    view.item_by_id(id)
        .expect("the item exists")
        .borrow()
        .height
}

/// The layout half of `computeRenderRangeAndEmit`.
fn recompute(view: &View) {
    let pending = {
        let mut state = view.state.borrow_mut();
        let reset = state.pending_layout_reset.take();
        state.layout_dirty_index.take().map(|index| (index, reset))
    };
    if let Some((index, reset)) = pending {
        view.recompute_layout(index, reset);
    }
}

fn selection(id: &str, start: usize, end: usize) -> CodeViewLineSelection {
    CodeViewLineSelection {
        id: id.to_owned(),
        range: SelectedLineRange {
            start,
            side: None,
            end,
            end_side: None,
        },
    }
}

fn silent() -> SelectionWriteOptions {
    SelectionWriteOptions {
        notify: Some(false),
        ..SelectionWriteOptions::default()
    }
}

// ---- CodeView.removeItem.test.ts ------------------------------------------------

#[test]
fn code_view_remove_item_removes_an_item_while_preserving_the_remaining_order() {
    let Harness { view, .. } = harness();
    view.set_items(&[
        diff_item("first", None, None),
        diff_item("middle", None, None),
        diff_item("last", None, None),
    ]);

    assert!(view.remove_item("middle"));

    assert!(view.get_item("middle").is_none());
    assert_eq!(ids(&view), ["first", "last"]);
    // The render half asserts `getRenderedItems()` (C5); the model keeps the
    // records' indexes in step.
    let indexes: Vec<usize> = view
        .state
        .borrow()
        .items
        .iter()
        .map(|item| item.borrow().index)
        .collect();
    assert_eq!(indexes, [0, 1]);
}

#[test]
fn code_view_remove_item_emits_a_null_selection_change_when_removing_the_selected_item() {
    let Harness { view, changes, .. } = harness();
    view.set_items(&[
        diff_item("kept", None, None),
        diff_item("selected", None, None),
    ]);
    view.set_selected_lines(Some(selection("selected", 2, 3)), silent());

    // Removing an unrelated item leaves the selection alone and stays silent.
    assert!(view.remove_item("kept"));
    assert!(changes.borrow().is_empty());
    assert_eq!(
        view.get_selected_lines().map(|selected| selected.id),
        Some("selected".to_owned())
    );

    // Removing the selected item clears it and tells the consumer, so
    // controlled selection state can't write the dead selection back.
    assert!(view.remove_item("selected"));
    assert_eq!(*changes.borrow(), [None]);
    assert_eq!(view.get_selected_lines(), None);
}

#[test]
fn code_view_remove_item_returns_false_for_an_unknown_item_id() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("existing", None, None)]);

    assert!(!view.remove_item("missing"));
    assert!(view.get_item("existing").is_some());
}

// ---- CodeView.updateItemId.test.ts ----------------------------------------------

#[test]
fn code_view_item_id_updates_emits_a_selected_line_change_when_renaming_the_selected_item() {
    let Harness { view, changes, .. } = harness();
    let renamed = selection("file:new", 2, 3);
    view.set_items(&[diff_item("file:old", None, None)]);
    view.set_selected_lines(Some(selection("file:old", 2, 3)), silent());
    let instance_before = Rc::clone(
        view.item_by_id("file:old")
            .expect("the item")
            .borrow()
            .instance(),
    );

    assert!(view.update_item_id("file:old", "file:new"));

    // The rename rebinds the existing record in place rather than removing and
    // re-adding it, so the same item instance is now reachable only under the
    // new id (the rendered element is C5's half of this case).
    let instance_after = Rc::clone(
        view.item_by_id("file:new")
            .expect("the item")
            .borrow()
            .instance(),
    );
    assert!(Rc::ptr_eq(&instance_before, &instance_after));
    assert_eq!(ids(&view), ["file:new"]);
    assert!(matches!(
        view.get_item("file:new"),
        Some(CodeViewItem::Diff(item)) if item.id == "file:new"
    ));
    assert!(view.get_item("file:old").is_none());
    assert_eq!(view.get_selected_lines(), Some(renamed.clone()));
    assert_eq!(*changes.borrow(), [Some(renamed)]);
}

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

// ---- CodeView.collapsed.test.ts (the layout half) ---------------------------------

#[test]
fn code_view_item_collapsed_state_mounts_mixed_initially_collapsed_and_expanded_items() {
    let Harness { view, .. } = harness();
    view.set_items(&[
        diff_item("diff:collapsed.txt", Some(true), None),
        diff_item("diff:expanded.txt", None, None),
    ]);
    recompute(&view);

    assert!(height_of(&view, "diff:collapsed.txt") < height_of(&view, "diff:expanded.txt"));
}

#[test]
fn code_view_item_collapsed_state_mounts_an_initially_collapsed_diff() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("diff:collapsed.txt", Some(true), None)]);
    let collapsed = height_of(&view, "diff:collapsed.txt");

    let Harness {
        view: expanded_view,
        ..
    } = harness();
    expanded_view.set_items(&[diff_item("diff:collapsed.txt", None, None)]);

    assert!(collapsed < height_of(&expanded_view, "diff:collapsed.txt"));
    // A collapsed diff is its header and padding: the item is that tall.
    assert_eq!(view.get_scroll_height(), collapsed);
}

#[test]
fn code_view_item_collapsed_state_collapses_an_item_when_its_versioned_snapshot_changes() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("file:example.txt", None, Some(0))]);
    recompute(&view);
    let expanded_height = height_of(&view, "file:example.txt");

    view.set_items(&[diff_item("file:example.txt", Some(true), Some(1))]);
    recompute(&view);

    assert!(height_of(&view, "file:example.txt") < expanded_height);
    assert_eq!(
        view.get_scroll_height(),
        height_of(&view, "file:example.txt")
    );
}

#[test]
fn code_view_item_collapsed_state_ignores_same_version_collapsed_changes() {
    let Harness { view, .. } = harness();
    view.set_items(&[diff_item("file:example.txt", None, Some(0))]);
    recompute(&view);
    let expanded_height = height_of(&view, "file:example.txt");

    view.set_items(&[diff_item("file:example.txt", Some(true), Some(0))]);
    recompute(&view);

    assert_eq!(height_of(&view, "file:example.txt"), expanded_height);
    assert!(matches!(
        view.get_item("file:example.txt"),
        Some(CodeViewItem::Diff(item)) if item.collapsed.is_none()
    ));
}

#[test]
fn code_view_item_collapsed_state_updates_one_item_without_changing_item_order() {
    let Harness { view, .. } = harness();
    view.set_items(&[
        diff_item("file:first.txt", None, Some(0)),
        diff_item("file:middle.txt", None, Some(0)),
        diff_item("file:last.txt", None, Some(0)),
    ]);
    recompute(&view);
    let expanded_height = height_of(&view, "file:middle.txt");
    let last_top = view
        .item_by_id("file:last.txt")
        .expect("the item")
        .borrow()
        .top;

    let Some(CodeViewItem::Diff(mut middle)) = view.get_item("file:middle.txt") else {
        panic!("the item exists");
    };
    middle.collapsed = Some(true);
    middle.version = Some(1);
    assert!(view.update_item(&CodeViewItem::Diff(middle)));
    recompute(&view);

    assert_eq!(
        ids(&view),
        ["file:first.txt", "file:middle.txt", "file:last.txt"]
    );
    assert!(height_of(&view, "file:middle.txt") < expanded_height);
    // The items after it moved up by what it gave up.
    assert!(
        view.item_by_id("file:last.txt")
            .expect("the item")
            .borrow()
            .top
            < last_top
    );
}

#[test]
fn code_view_item_collapsed_state_keeps_rendering_after_many_collapsed_items_shrink_the_layout() {
    let Harness { view, .. } = harness();
    let items = |collapsed: Option<bool>, version: usize| -> Vec<CodeViewItem<u8>> {
        (0..40)
            .map(|index| diff_item(&format!("file:{index}"), collapsed, Some(version)))
            .collect()
    };
    view.set_items(&items(None, 0));
    recompute(&view);
    let expanded_scroll_height = view.get_scroll_height();

    view.set_items(&items(Some(true), 1));
    recompute(&view);

    // Every item is identical and collapsed, so the total content height is the
    // item count times one collapsed item's height plus the gaps between them
    // (the render half asserts the window lands on the tail, C5).
    let collapsed_height = height_of(&view, "file:0");
    let gap = view.get_layout().gap;
    assert_eq!(
        view.get_scroll_height(),
        40.0 * collapsed_height + 39.0 * gap
    );
    assert!(view.get_scroll_height() < expanded_scroll_height);
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
