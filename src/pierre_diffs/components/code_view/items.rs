// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C2: the item operations: `updateItem`, `updateItemId`, `addItem(s)`,
// `removeItem`, `setItems`, `appendItemsInternal`, `tryAppendItems`,
// `reconcileItems`, `syncItemRecord`, `createItem`, `releaseRenderedItem`,
// `renamePendingScrollTarget`, `renamePendingLayoutAnchor`).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * Inputs are `&[CodeViewItem<LAnnotation>]`; items are cloned into the
//   records (`CodeViewItem` is cheap to clone: its diff and annotations are
//   `Rc`s).
// * `console.error` on unknown ids is dropped: `updateItem`, `updateItemId`
//   and `removeItem` return `false`. A duplicate id in `addItems`/`setItems`
//   throws upstream and panics here.
// * `syncItemEditors` and the edit-session bookkeeping around `setItems` are
//   DROP (edit mode). `setItems` returns nothing: `reconcileItems` hands the
//   removed items back to be cleaned up after the render is queued, as
//   upstream's `finally` does.
// * A file item reaches `createItem`'s file arm, which `todo!`s: `VirtualizedFile`
//   is DEFER (Q1).
// * `releaseRenderedItem` is only the part that does not touch an element
//   (`instance.cleanUp(true)`): a rendered item's shell goes back to the pool,
//   which is C4 (`todo!`); no item has an element before then.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use super::CodeView;
use super::CodeViewHost;
use super::state::ContextItem;
use super::state::DiffItemContext;
use super::state::ItemKind;
use super::state::ItemOptionsState;
use super::state::ItemRef;
use crate::components::file_diff::RenderedContent;
use crate::components::virtualized_file_diff::advanced::AdvancedVirtualizer;
use crate::components::virtualized_file_diff::advanced::VirtualizerKind;
use crate::components::virtualized_file_diff::instance::VirtualizedFileDiff;
use crate::types::view::CodeViewItem;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use std::rc::Weak;

impl<Host, Rendered, LAnnotation> CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `updateItem(input)`
    pub fn update_item(&self, input: &CodeViewItem<LAnnotation>) -> bool {
        let Some(item) = self.item_by_id(input_id(input)) else {
            return false;
        };
        if !self.sync_item_record(&item, input) {
            return false;
        }

        self.mark_item_layout_dirty(&item);
        self.state.borrow_mut().scroll_dirty = true;
        self.render(false);
        self.sync_selection();
        true
    }

    /// `updateItemId(oldId, newId)`: rebinds the record in place, so the
    /// instance (and its element) is reachable only under the new id.
    pub fn update_item_id(&self, old_id: &str, new_id: &str) -> bool {
        if old_id == new_id {
            return true;
        }
        let Some(item) = self.item_by_id(old_id) else {
            return false;
        };
        if self.item_by_id(new_id).is_some() {
            return false;
        }

        {
            let mut state = self.state.borrow_mut();
            state.id_to_item.remove(old_id);
            state.id_to_item.insert(new_id.to_owned(), Rc::clone(&item));
        }
        match &mut item.borrow_mut().kind {
            ItemKind::Diff(diff) => {
                diff.item.id = new_id.to_owned();
                *diff.options_state.id.borrow_mut() = new_id.to_owned();
            }
            ItemKind::File(never) => match *never {},
        }

        let renamed_selection = {
            let mut state = self.state.borrow_mut();
            match &mut state.selected_lines {
                Some(selected) if selected.id == old_id => {
                    selected.id = new_id.to_owned();
                    Some(selected.clone())
                }
                _ => None,
            }
        };
        if let Some(selection) = renamed_selection {
            let on_change = self.state.borrow().options.on_selected_lines_change.clone();
            if let Some(on_change) = on_change {
                on_change(Some(&selection));
            }
        }
        self.rename_pending_scroll_target(old_id, new_id);
        self.rename_pending_layout_anchor(old_id, new_id);
        self.render(false);
        true
    }

    /// `addItem(input)`
    pub fn add_item(&self, input: &CodeViewItem<LAnnotation>) {
        self.add_items(std::slice::from_ref(input));
    }

    /// `addItems(inputs)`
    pub fn add_items(&self, inputs: &[CodeViewItem<LAnnotation>]) {
        self.append_items_internal(inputs, true);
        self.sync_selection();
    }

    /// `removeItem(itemId)`
    pub fn remove_item(&self, item_id: &str) -> bool {
        let Some(item) = self.item_by_id(item_id) else {
            return false;
        };
        let next_items: Vec<CodeViewItem<LAnnotation>> = self
            .state
            .borrow()
            .items
            .iter()
            .filter(|current| !Rc::ptr_eq(current, &item))
            .map(|current| current.borrow().snapshot())
            .collect();
        self.set_items(&next_items);
        true
    }

    /// `setItems(items)`
    pub fn set_items(&self, items: &[CodeViewItem<LAnnotation>]) {
        let mut removed_items = Vec::new();
        if self.state.borrow().items.is_empty() {
            self.append_items_internal(items, true);
        } else if !self.try_append_items(items) {
            removed_items = self.reconcile_items(items);
        }
        for removed_item in removed_items {
            let instance = Rc::clone(removed_item.borrow().instance());
            instance.clean_up(false);
        }
        self.sync_selection();
    }

    /// Append new records to the viewer while preserving existing layout
    /// state. This is the shared path for imperative adds and the append-only
    /// reconcile fast path, so it measures new items immediately and only
    /// triggers render once at the end. `appendItemsInternal(inputs, render)`
    pub(crate) fn append_items_internal(&self, inputs: &[CodeViewItem<LAnnotation>], render: bool) {
        if inputs.is_empty() {
            return;
        }

        let layout = self.get_layout();
        let mut next_top = {
            let state = self.state.borrow();
            if state.items.is_empty() {
                0.0
            } else {
                state.scroll_height + layout.gap
            }
        };
        let appended_top = next_top;
        for input in inputs {
            let id = input_id(input);
            assert!(
                self.item_by_id(id).is_none(),
                "CodeView.addItem: duplicate id \"{id}\""
            );

            let index = self.state.borrow().items.len();
            let item = self.create_item(input, index, next_top);
            {
                let mut state = self.state.borrow_mut();
                state.items.push(Rc::clone(&item));
                state.id_to_item.insert(id.to_owned(), Rc::clone(&item));
                let key = item.borrow().instance_key();
                state.instance_to_item.insert(key, Rc::clone(&item));
            }
            let height = self.prepare_item_instance(&item);
            item.borrow_mut().height = height;
            next_top += height + layout.gap;
        }

        {
            let mut state = self.state.borrow_mut();
            state.scroll_height = next_top - layout.gap;
            state.scroll_dirty = true;
        }
        if render {
            if self.can_skip_render_for_append(appended_top) {
                self.sync_container_height();
            } else {
                self.render(false);
            }
        }
    }

    /// Detect the common controlled-update case where the new list simply
    /// extends the existing ordered prefix. When that happens we can reuse
    /// every current record in place, sync any versioned payload changes, and
    /// append only the new tail instead of rebuilding the whole list.
    /// `tryAppendItems(items)`
    fn try_append_items(&self, items: &[CodeViewItem<LAnnotation>]) -> bool {
        let existing_items = self.state.borrow().items.clone();
        if items.len() <= existing_items.len() {
            return false;
        }

        for (existing_item, next_item) in existing_items.iter().zip(items) {
            let existing_item = existing_item.borrow();
            if existing_item.id() != input_id(next_item) || !existing_item.has_type_of(next_item) {
                return false;
            }
        }

        for (index, (existing_item, next_item)) in existing_items.iter().zip(items).enumerate() {
            if self.sync_item_record(existing_item, next_item) {
                self.mark_layout_dirty_from_index(index);
            }
        }

        self.append_items_internal(&items[existing_items.len()..], false);
        self.state.borrow_mut().scroll_dirty = true;
        self.render(false);
        true
    }

    /// Reconcile a new controlled item list against the existing records by
    /// id. This reuses records and instances when type matches, cleans up
    /// removed records, rebuilds the lookup maps, and marks layout dirty
    /// whenever order, membership, or versioned item data changes. Returns the
    /// removed records (`removedItemsById.values()`).
    /// `reconcileItems(items)`
    fn reconcile_items(
        &self,
        items: &[CodeViewItem<LAnnotation>],
    ) -> Vec<ItemRef<Host, Rendered, LAnnotation>> {
        let (previous_items, previous_by_id) = {
            let state = self.state.borrow();
            (state.items.clone(), state.id_to_item.clone())
        };
        let item_key = |item: &ItemRef<Host, Rendered, LAnnotation>| Rc::as_ptr(item) as usize;
        let mut removed_items: HashSet<usize> = previous_items.iter().map(item_key).collect();
        let mut next_items: Vec<ItemRef<Host, Rendered, LAnnotation>> =
            Vec::with_capacity(items.len());
        let mut next_id_to_item: HashMap<String, ItemRef<Host, Rendered, LAnnotation>> =
            HashMap::new();
        let mut next_instance_to_item: HashMap<usize, ItemRef<Host, Rendered, LAnnotation>> =
            HashMap::new();
        let mut first_dirty_index: Option<usize> = None;
        let mark_dirty = |first_dirty_index: &mut Option<usize>, index: usize| {
            *first_dirty_index = Some(first_dirty_index.map_or(index, |dirty| dirty.min(index)));
        };

        for (index, input) in items.iter().enumerate() {
            let id = input_id(input);
            assert!(
                !next_id_to_item.contains_key(id),
                "CodeView.setItems: duplicate id \"{id}\""
            );

            let previous_item = previous_by_id.get(id);
            let reusable = previous_item.filter(|previous| previous.borrow().has_type_of(input));
            let item = match reusable {
                Some(previous) => Rc::clone(previous),
                None => self.create_item(input, index, 0.0),
            };

            item.borrow_mut().index = index;

            if let Some(previous) = reusable {
                removed_items.remove(&item_key(previous));
                if self.sync_item_record(&item, input) {
                    mark_dirty(&mut first_dirty_index, index);
                }
            } else {
                mark_dirty(&mut first_dirty_index, index);
            }

            if previous_items
                .get(index)
                .is_none_or(|previous| !Rc::ptr_eq(previous, &item))
            {
                mark_dirty(&mut first_dirty_index, index);
            }

            let key = item.borrow().instance_key();
            next_instance_to_item.insert(key, Rc::clone(&item));
            next_id_to_item.insert(id.to_owned(), Rc::clone(&item));
            next_items.push(item);
        }

        let mut first_dirty_index = match first_dirty_index {
            Some(index) => index,
            None => {
                if removed_items.is_empty() {
                    return Vec::new();
                }
                next_items.len().saturating_sub(1)
            }
        };

        self.capture_pending_layout_anchor(Some(&next_id_to_item));

        let mut removed_items_by_id: Vec<ItemRef<Host, Rendered, LAnnotation>> = Vec::new();
        for removed_item in &previous_items {
            if !removed_items.contains(&item_key(removed_item)) {
                continue;
            }
            removed_items_by_id.push(Rc::clone(removed_item));
            self.release_rendered_item(removed_item);
            let dirty_index = next_items.len().saturating_sub(1);
            first_dirty_index = first_dirty_index.min(dirty_index);
        }

        let next_len = next_items.len();
        {
            let mut state = self.state.borrow_mut();
            state.items = next_items;
            state.id_to_item = next_id_to_item;
            state.instance_to_item = next_instance_to_item;

            if state.render_state.first_index >= next_len as i64 {
                state.render_state = super::state::RenderState::RESET;
            } else if state.render_state.last_index >= next_len as i64 {
                state.render_state.last_index = next_len as i64 - 1;
            }
        }

        self.mark_layout_dirty_from_index(first_dirty_index);
        self.state.borrow_mut().scroll_dirty = true;
        self.render(false);
        removed_items_by_id
    }

    /// Update a reused record from the latest controlled item only when its
    /// item version changes. Matching versions mean CodeView keeps the current
    /// record snapshot, which lets imperative updates remain in place until the
    /// caller intentionally publishes a newer version.
    /// `syncItemRecord(item, nextItem)`
    pub(crate) fn sync_item_record(
        &self,
        item_ref: &ItemRef<Host, Rendered, LAnnotation>,
        next_item: &CodeViewItem<LAnnotation>,
    ) -> bool {
        let mut item = item_ref.borrow_mut();
        assert!(
            item.has_type_of(next_item),
            "CodeView.syncItemRecord: type mismatch for id \"{}\"",
            input_id(next_item)
        );

        let next_version = match next_item {
            CodeViewItem::Diff(next) => next.version,
            CodeViewItem::File(next) => next.version,
        };
        if item.version == next_version {
            return false;
        }

        match (&mut item.kind, next_item) {
            (ItemKind::Diff(diff), CodeViewItem::Diff(next)) => diff.item = next.clone(),
            (ItemKind::File(never), _) => match *never {},
            (ItemKind::Diff(_), CodeViewItem::File(_)) => unreachable!("checked above"),
        }
        item.version = next_version;
        item.rendered_options_revision = -1;
        drop(item);
        // Upstream's per-item options read the record live (`collapsed`, the
        // callbacks' item), so the instance sees the new snapshot at once: the
        // anchor `reconcileItems` captures next asks the instance for its line
        // positions, which a collapsed item does not have.
        self.push_item_options(item_ref);
        true
    }

    /// `createItem(input, index, top)`
    fn create_item(
        &self,
        input: &CodeViewItem<LAnnotation>,
        index: usize,
        top: f64,
    ) -> ItemRef<Host, Rendered, LAnnotation> {
        let (item_metrics, render_options_revision) = {
            let state = self.state.borrow();
            (state.item_metrics_cache, state.render_options_revision)
        };
        let CodeViewItem::Diff(input) = input else {
            todo!("file items: VirtualizedFile is DEFER (closure notes, Q1)");
        };
        let options_state = Rc::new(ItemOptionsState {
            id: RefCell::new(input.id.clone()),
        });
        let mut callbacks = Default::default();
        let options = self.build_diff_options(input, &options_state, &mut callbacks);
        let weak_self: Weak<Self> = Weak::clone(&self.weak_self);
        let virtualizer: Weak<dyn AdvancedVirtualizer> = weak_self;
        let instance = VirtualizedFileDiff::new(
            Rc::clone(&self.host),
            Rc::clone(&self.resize_observer_hub),
            self.render_queue.clone(),
            Rc::clone(&self.highlighter),
            options,
            VirtualizerKind::Advanced(virtualizer),
            PartialVirtualFileMetrics::from(item_metrics),
            None,
        );
        Rc::new(RefCell::new(ContextItem {
            index,
            top,
            height: 0.0,
            element: None,
            version: input.version,
            rendered_options_revision: render_options_revision,
            kind: ItemKind::Diff(DiffItemContext {
                item: input.clone(),
                instance,
                options_state,
                callbacks,
            }),
        }))
    }

    /// `renamePendingScrollTarget(oldId, newId)`
    fn rename_pending_scroll_target(&self, old_id: &str, new_id: &str) {
        let mut state = self.state.borrow_mut();
        let renamed = match &state.pending_scroll_target {
            Some(target) if target.id() == Some(old_id) => target.with_id(new_id.to_owned()),
            _ => return,
        };
        state.pending_scroll_target = Some(renamed);
    }

    /// `renamePendingLayoutAnchor(oldId, newId)`
    fn rename_pending_layout_anchor(&self, old_id: &str, new_id: &str) {
        let mut state = self.state.borrow_mut();
        if let Some(anchor) = &mut state.pending_layout_anchor
            && anchor.id() == old_id
        {
            anchor.set_id(new_id.to_owned());
        }
    }
}

/// `input.id`
fn input_id<LAnnotation>(input: &CodeViewItem<LAnnotation>) -> &str {
    match input {
        CodeViewItem::Diff(item) => &item.id,
        CodeViewItem::File(item) => &item.id,
    }
}
