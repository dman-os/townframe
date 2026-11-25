// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/CodeView.removeItem.test.ts, packages/diffs/test/CodeView.updateItemId.test.ts, packages/diffs/test/CodeView.collapsed.test.ts
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; file items become diff items (divergences.psv).

//! Upstream's `CodeView` cases that follow an item through the render pass and
//! its model (C2 ported the model half, C5 makes them whole), with their names
//! (prefixed by their `describe`):
//! * `test/CodeView.removeItem.test.ts`, 3 of 3
//! * `test/CodeView.updateItemId.test.ts`, 2 of 2
//! * `test/CodeView.collapsed.test.ts`, 6 of 6
//!
//! The upstream cases build file items (`makeFileItem`, `makeFile`), which are
//! DEFER with `VirtualizedFile`: the fixtures are diff items (harness_tests.rs,
//! parity/divergences.psv), so every height differs from upstream's and the
//! assertions that compare heights are the relations upstream asserts (shorter
//! than expanded, bounded by N collapsed items), not numbers. The collapsed
//! cases of the file kind (`collapsedFile.txt`, `getRenderedFile`) are the diff
//! kind here: `getRenderedFile(instance)` / `getRenderedDiff(instance)` is the
//! instance's input diff, compared by identity. The last collapsed case runs on a
//! clamping root (see the case). `console.error`'s message is
//! not asserted: the port logs nothing. Test-only.

use std::cell::RefCell;
use std::rc::Rc;

use super::harness_tests::Page;
use super::harness_tests::file_like_item;
use super::harness_tests::page;
use super::harness_tests::shadow_count;
use super::harness_tests::small_diff_item;
use super::harness_tests::versioned;
use super::options::CodeViewOptions;
use super::state::CodeViewLineSelection;
use super::state::ItemRef;
use crate::constants::DEFAULT_CODE_VIEW_LAYOUT;
use crate::fake_interaction_dom::FakeDom;
use crate::types::view::CodeViewItem;
use crate::types::view::SelectedLineRange;

type Item = ItemRef<FakeDom, u8, u8>;

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

fn silent() -> crate::managers::interaction_manager::SelectionWriteOptions {
    crate::managers::interaction_manager::SelectionWriteOptions {
        notify: Some(false),
        ..Default::default()
    }
}

/// A viewer that records the selection changes it reported.
fn recording_page(height: f64) -> (Page, Rc<RefCell<Vec<Option<CodeViewLineSelection>>>>) {
    let changes: Rc<RefCell<Vec<Option<CodeViewLineSelection>>>> = Rc::default();
    let recorded = Rc::clone(&changes);
    let page = page(
        CodeViewOptions {
            on_selected_lines_change: Some(Rc::new(move |selection| {
                recorded.borrow_mut().push(selection.cloned());
            })),
            ..CodeViewOptions::default()
        },
        height,
    );
    (page, changes)
}

fn id_of(item: &Item) -> String {
    item.borrow().id().to_owned()
}

// ---- CodeView.removeItem.test.ts ------------------------------------------------

#[test]
fn code_view_remove_item_removes_an_item_while_preserving_the_remaining_order() {
    let page = page(CodeViewOptions::default(), 800.0);
    page.render_items(&[
        file_like_item("first", "line", 5),
        file_like_item("middle", "line", 5),
        file_like_item("last", "line", 5),
    ]);

    assert!(page.view.remove_item("middle"));
    page.view.render(true);

    assert!(page.view.get_item("middle").is_none());
    assert_eq!(page.rendered_ids(), ["first", "last"]);
    // The model keeps the records' indexes in step.
    let indexes: Vec<usize> = page
        .view
        .state
        .borrow()
        .items
        .iter()
        .map(|item| item.borrow().index)
        .collect();
    assert_eq!(indexes, [0, 1]);
    page.finish();
}

#[test]
fn code_view_remove_item_emits_a_null_selection_change_when_removing_the_selected_item() {
    let (page, changes) = recording_page(800.0);
    page.render_items(&[
        file_like_item("kept", "line", 5),
        file_like_item("selected", "line", 5),
    ]);
    page.view
        .set_selected_lines(Some(selection("selected", 2, 3)), silent());

    // Removing an unrelated item leaves the selection alone and stays silent.
    assert!(page.view.remove_item("kept"));
    assert!(changes.borrow().is_empty());
    assert_eq!(
        page.view.get_selected_lines().map(|selected| selected.id),
        Some("selected".to_owned())
    );

    // Removing the selected item clears it and tells the consumer, so
    // controlled selection state can't write the dead selection back.
    assert!(page.view.remove_item("selected"));
    assert_eq!(*changes.borrow(), [None]);
    assert_eq!(page.view.get_selected_lines(), None);
    page.finish();
}

#[test]
fn code_view_remove_item_returns_false_for_an_unknown_item_id() {
    let page = page(CodeViewOptions::default(), 800.0);
    page.render_items(&[file_like_item("existing", "line", 5)]);

    assert!(!page.view.remove_item("missing"));
    assert!(page.view.get_item("existing").is_some());
    page.finish();
}

// ---- CodeView.updateItemId.test.ts ----------------------------------------------

#[test]
fn code_view_item_id_updates_emits_a_selected_line_change_when_renaming_the_selected_item() {
    let (page, changes) = recording_page(800.0);
    let renamed = selection("file:new", 2, 3);
    page.render_items(&[file_like_item("file:old", "line", 20)]);
    page.view
        .set_selected_lines(Some(selection("file:old", 2, 3)), silent());
    let rendered_before = page.rendered_items();
    assert_eq!(page.rendered_ids(), ["file:old"]);

    assert!(page.view.update_item_id("file:old", "file:new"));

    // The rename rebinds the existing record in place rather than removing and
    // re-adding it, so the same rendered element and item instance must now be
    // reachable only under the new id.
    let rendered_after = page.rendered_items();
    assert_eq!(page.rendered_ids(), ["file:new"]);
    assert_eq!(
        rendered_after[0].borrow().element,
        rendered_before[0].borrow().element
    );
    assert!(Rc::ptr_eq(
        rendered_after[0].borrow().instance(),
        rendered_before[0].borrow().instance()
    ));
    assert!(matches!(
        page.view.get_item("file:new"),
        Some(CodeViewItem::Diff(item)) if item.id == "file:new"
    ));
    assert!(page.view.get_item("file:old").is_none());
    assert_eq!(page.view.get_selected_lines(), Some(renamed.clone()));
    assert_eq!(*changes.borrow(), [Some(renamed)]);
    page.finish();
}

#[test]
fn code_view_item_id_updates_does_not_mutate_a_pending_scroll_target_passed_by_the_caller() {
    let page = page(CodeViewOptions::default(), 800.0);
    let target =
        super::harness_tests::item_target("file:old", crate::types::view::ScrollAlign::Center);
    let untouched = target.clone();
    page.render_items(&[file_like_item("file:old", "line", 120)]);

    page.view.scroll_to(&target);
    assert!(page.view.update_item_id("file:old", "file:new"));
    assert_eq!(target, untouched);

    // The pending scroll has not been applied yet (scrollTo defers to the next
    // render frame), so the rename must retarget it to the new id for the
    // forced render below to scroll at all.
    assert_eq!(page.scroll_top(), 0.0);
    page.view.render(true);
    // The 120-line item is taller than the viewport, so center alignment falls
    // back to start: the item's top (0) plus the layout padding. A stale
    // 'file:old' target would resolve to no item and leave scrollTop at 0.
    assert_eq!(page.scroll_top(), DEFAULT_CODE_VIEW_LAYOUT.padding_top);
    assert_eq!(page.rendered_ids(), ["file:new"]);
    page.finish();
}

// ---- CodeView.collapsed.test.ts -------------------------------------------------

/// `hasRenderedCode(item)`: the item's shadow root holds a `<pre>`.
fn has_rendered_code(page: &Page, item: &Item) -> bool {
    let element = item
        .borrow()
        .element
        .expect("a rendered item holds a shell");
    shadow_count(&page.dom, element, "pre", None) > 0
}

/// `getRenderedDiff(instance)`: the diff the instance holds as its input.
fn rendered_diff(item: &Item) -> Option<Rc<crate::types::patch::FileDiffMetadata>> {
    let instance = Rc::clone(item.borrow().instance());
    instance.file_diff().get_input_diff()
}

fn item_diff(item: &CodeViewItem<u8>) -> Rc<crate::types::patch::FileDiffMetadata> {
    match item {
        CodeViewItem::Diff(diff) => Rc::clone(&diff.file_diff),
        CodeViewItem::File(_) => unreachable!("the fixtures are diff items"),
    }
}

fn rendered(page: &Page, id: &str) -> Item {
    page.rendered_items()
        .into_iter()
        .find(|item| id_of(item) == id)
        .unwrap_or_else(|| panic!("{id} is rendered"))
}

#[test]
fn code_view_item_collapsed_state_mounts_mixed_initially_collapsed_and_expanded_items() {
    let page = page(CodeViewOptions::default(), 800.0);
    let collapsed_item = versioned(
        &small_diff_item("diff:collapsed.txt", "collapsed.txt"),
        Some(true),
        None,
    );
    page.render_items(&[
        collapsed_item.clone(),
        small_diff_item("diff:expanded.txt", "expanded.txt"),
    ]);

    let collapsed = rendered(&page, "diff:collapsed.txt");
    let expanded = rendered(&page, "diff:expanded.txt");

    assert!(!has_rendered_code(&page, &collapsed));
    assert!(has_rendered_code(&page, &expanded));
    assert!(Rc::ptr_eq(
        &rendered_diff(&collapsed).expect("the collapsed item holds its diff"),
        &item_diff(&collapsed_item)
    ));
    page.finish();
}

#[test]
fn code_view_item_collapsed_state_mounts_an_initially_collapsed_diff() {
    let page = page(CodeViewOptions::default(), 800.0);
    let item = versioned(
        &small_diff_item("diff:collapsed.txt", "collapsed.txt"),
        Some(true),
        None,
    );
    page.render_items(std::slice::from_ref(&item));

    let rendered_item = page
        .rendered_items()
        .into_iter()
        .next()
        .expect("an item is rendered");
    assert!(!has_rendered_code(&page, &rendered_item));
    assert!(Rc::ptr_eq(
        &rendered_diff(&rendered_item).expect("the item holds its diff"),
        &item_diff(&item)
    ));
    page.finish();
}

#[test]
fn code_view_item_collapsed_state_collapses_an_item_when_its_versioned_snapshot_changes() {
    let page = page(CodeViewOptions::default(), 800.0);
    let item = versioned(
        &file_like_item("file:example.txt", "line", 20),
        None,
        Some(0),
    );
    page.render_items(std::slice::from_ref(&item));

    let expanded_item = page
        .rendered_items()
        .into_iter()
        .next()
        .expect("an item is rendered");
    assert!(has_rendered_code(&page, &expanded_item));
    let expanded_height = expanded_item.borrow().instance().get_virtualized_height();

    page.render_items(&[versioned(&item, Some(true), Some(1))]);

    let collapsed_item = page
        .rendered_items()
        .into_iter()
        .next()
        .expect("an item is rendered");
    assert!(!has_rendered_code(&page, &collapsed_item));
    assert!(Rc::ptr_eq(
        &rendered_diff(&collapsed_item).expect("the item holds its diff"),
        &item_diff(&item)
    ));
    assert!(collapsed_item.borrow().instance().get_virtualized_height() < expanded_height);
    page.finish();
}

#[test]
fn code_view_item_collapsed_state_ignores_same_version_collapsed_changes() {
    let page = page(CodeViewOptions::default(), 800.0);
    let item = versioned(
        &file_like_item("file:example.txt", "line", 20),
        None,
        Some(0),
    );
    page.render_items(std::slice::from_ref(&item));

    page.render_items(&[versioned(&item, Some(true), Some(0))]);

    let rendered_item = page
        .rendered_items()
        .into_iter()
        .next()
        .expect("an item is rendered");
    assert!(has_rendered_code(&page, &rendered_item));
    page.finish();
}

#[test]
fn code_view_item_collapsed_state_updates_one_item_without_changing_item_order() {
    let page = page(CodeViewOptions::default(), 800.0);
    let items: Vec<CodeViewItem<u8>> = ["first", "middle", "last"]
        .iter()
        .map(|name| {
            versioned(
                &file_like_item(&format!("file:{name}.txt"), "line", 20),
                None,
                Some(0),
            )
        })
        .collect();
    page.render_items(&items);

    let Some(CodeViewItem::Diff(mut middle)) = page.view.get_item("file:middle.txt") else {
        panic!("the item exists");
    };
    middle.collapsed = Some(true);
    middle.version = Some(1);

    assert!(page.view.update_item(&CodeViewItem::Diff(middle)));
    page.view.render(true);
    page.wait_a_frame();

    assert_eq!(
        page.rendered_ids(),
        ["file:first.txt", "file:middle.txt", "file:last.txt"]
    );
    let rendered_middle = &page.rendered_items()[1];
    assert!(!has_rendered_code(&page, rendered_middle));
    page.finish();
}

#[test]
fn code_view_item_collapsed_state_keeps_rendering_after_many_collapsed_items_shrink_the_layout() {
    let page = page(CodeViewOptions::default(), 800.0);
    let items: Vec<CodeViewItem<u8>> = (0..40)
        .map(|index| {
            versioned(
                &file_like_item(&format!("file:{index}"), "line", 30),
                None,
                Some(0),
            )
        })
        .collect();
    page.render_items(&items);

    page.scroll_and_render(20_000.0);

    let collapsed_items: Vec<CodeViewItem<u8>> = items
        .iter()
        .map(|item| versioned(item, Some(true), Some(1)))
        .collect();

    page.render_items(&collapsed_items);

    let rendered_items = page.rendered_items();
    assert!(!rendered_items.is_empty());
    // The clamped scroll position must land the render window on the tail of
    // the list rather than stranding it past the shrunken content.
    assert!(
        page.rendered_ids()
            .contains(&format!("file:{}", items.len() - 1))
    );
    // Every item is identical and collapsed, so the total content height is the
    // item count times one collapsed item's virtualized height; the clamped
    // scroll offset cannot exceed it.
    let collapsed_height = rendered_items[0]
        .borrow()
        .instance()
        .get_virtualized_height();
    // The logical position: `root.scrollTop` is the browser's to clamp when the
    // content shrinks, and `createRoot`'s plain property never does. CodeView only
    // writes it when the position it resolves differs from the clamped one it
    // already holds, which this fixture's anchor does not (divergences.psv).
    assert!(page.view.get_scroll_top() <= items.len() as f64 * collapsed_height);
    page.finish();
}
