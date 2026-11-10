// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/managers/ResizeManager.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: DOM access (children, tag names, `firstElementChild`, the annotation `querySelectorAll`, attributes, bounding heights, style properties, `isConnected`, and the `ResizeObserver` calls) goes through `ResizeManagerHost`; the host returns `HTMLElement`s only, so the `instanceof HTMLElement` checks (and the "Invalid element for ResizeObserver" branch) are type-level. The class's statics (`resizeObserver`, `managersByElement`, `getResizeObserver`, `handleSharedResizeEntries`) are a `ResizeObserverHub` shared by `Rc` among the managers of one host; observer entries come back through `ResizeObserverHub::handle_shared_resize_entries`. `ObservedGridNodes` / `ObservedAnnotationNodes` (`types.ts`) are defined here, shared between their element keys by `Rc<RefCell<..>>` identity as the object references are. `console.error` calls (an invalid or unpaired annotation, an unknown observed node) are `ResizeManagerHost::report` and the code continues, as upstream does; a manager observing an element another owns panics (upstream throws). `/^-?\d+,-?\d+$/` is a hand-written check. `columnVariables` defaults to `Apply`. `setup` is split in two private methods, `setup_code_columns` and `setup_annotation_pairs` (its two halves; one function upstream), to stay inside the function length budget.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::document::Document;
use crate::document::NodeKey;
use crate::js_collections::InsertionMap;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CodeWidth {
    Auto,
    Px(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObservedGridNodes<Element> {
    pub code_element: Element,
    pub number_element: Option<Element>,
    pub code_width: CodeWidth,
    pub number_width: f64,
    pub apply_column_variables: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObservedAnnotationColumn<Element> {
    pub container: Element,
    pub child: Element,
    pub child_height: f64,
}

/// `currentHeight: number | 'auto'`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CurrentHeight {
    Auto,
    Px(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObservedAnnotationNodes<Element> {
    pub column1: ObservedAnnotationColumn<Element>,
    pub column2: ObservedAnnotationColumn<Element>,
    pub current_height: CurrentHeight,
}

type GridItem<Element> = Rc<RefCell<ObservedGridNodes<Element>>>;
type AnnotationItem<Element> = Rc<RefCell<ObservedAnnotationNodes<Element>>>;

/// `ObservedAnnotationNodes | ObservedGridNodes`
enum Observed<Element> {
    Code(GridItem<Element>),
    Annotations(AnnotationItem<Element>),
}

impl<Element> Clone for Observed<Element> {
    fn clone(&self) -> Self {
        match self {
            Self::Code(item) => Self::Code(Rc::clone(item)),
            Self::Annotations(item) => Self::Annotations(Rc::clone(item)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResizeManagerColumnVariableMode {
    #[default]
    Apply,
    Measure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResizeManagerSetupOptions {
    pub disable_annotations: bool,
    pub column_variables: ResizeManagerColumnVariableMode,
}

/// The parts of a `ResizeObserverEntry` the manager reads.
#[derive(Debug, Clone, PartialEq)]
pub struct ResizeObserverEntry<Element> {
    pub target: Element,
    /// `borderBoxSize[0].blockSize`
    pub border_box_block_size: f64,
    /// `contentBoxSize[0].inlineSize`
    pub content_box_inline_size: f64,
}

/// What the platform supplies: the rendered document, geometry, connectedness
/// and the resize observer. Structure and style writes are the `Document`'s.
pub trait ResizeManagerHost: 'static {
    fn document(&self) -> &Document;
    /// `element.getBoundingClientRect().height`
    fn bounding_height(&self, element: NodeKey) -> f64;
    fn is_connected(&self, element: NodeKey) -> bool;
    /// `new ResizeObserver(ResizeManager.handleSharedResizeEntries)`
    fn create_resize_observer(&self);
    fn observe(&self, element: NodeKey);
    fn unobserve(&self, element: NodeKey);
    /// `resizeObserver.disconnect()`
    fn disconnect_resize_observer(&self);
    /// `console.error(message, ...)`; `detail` is the annotation string or
    /// pair key the call also passes.
    fn report(&self, message: &str, detail: Option<&str>);
}

static MANAGER_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// The shared static state of `ResizeManager`.
pub struct ResizeObserverHub<Host: ResizeManagerHost> {
    host: Rc<Host>,
    observer_exists: RefCell<bool>,
    managers_by_element: RefCell<InsertionMap<NodeKey, usize>>,
    managers: RefCell<HashMap<usize, Weak<ResizeManager<Host>>>>,
}

impl<Host: ResizeManagerHost> ResizeObserverHub<Host> {
    pub fn new(host: Rc<Host>) -> Rc<Self> {
        Rc::new(Self {
            host,
            observer_exists: RefCell::new(false),
            managers_by_element: RefCell::default(),
            managers: RefCell::default(),
        })
    }

    // `getResizeObserver`: created on first use.
    fn ensure_resize_observer(&self) {
        let exists = *self.observer_exists.borrow();
        if !exists {
            self.host.create_resize_observer();
            *self.observer_exists.borrow_mut() = true;
        }
    }

    /// `handleSharedResizeEntries`, the observer callback.
    pub fn handle_shared_resize_entries(&self, entries: &[ResizeObserverEntry<NodeKey>]) {
        // First we need to batch all elements by manager, so callbacks are
        // properly aligned with a per-instance ResizeManager
        let mut entries_by_manager: InsertionMap<usize, Vec<ResizeObserverEntry<NodeKey>>> =
            InsertionMap::default();
        for entry in entries {
            let manager = self
                .managers_by_element
                .borrow()
                .get(&entry.target)
                .copied();
            let Some(manager) = manager else {
                continue;
            };
            match entries_by_manager.get_mut(&manager) {
                Some(manager_entries) => manager_entries.push(entry.clone()),
                None => entries_by_manager.insert(manager, vec![entry.clone()]),
            }
        }

        for (manager, manager_entries) in entries_by_manager.entries() {
            let manager = self.managers.borrow().get(&manager).and_then(Weak::upgrade);
            if let Some(manager) = manager {
                manager.handle_resize_entries(&manager_entries);
            }
        }
    }
}

#[derive(Clone, Copy)]
struct CodeColumnUpdate {
    code_inline_size: Option<f64>,
    number_inline_size: Option<f64>,
}

struct AnnotationSetup<Element> {
    child1: Element,
    child2: Element,
    item: AnnotationItem<Element>,
    new_height: f64,
}

fn item_key<T>(item: &Rc<T>) -> usize {
    Rc::as_ptr(item) as usize
}

pub struct ResizeManager<Host: ResizeManagerHost> {
    id: usize,
    host: Rc<Host>,
    hub: Rc<ResizeObserverHub<Host>>,
    observed_nodes: RefCell<InsertionMap<NodeKey, Observed<NodeKey>>>,
}

impl<Host: ResizeManagerHost> ResizeManager<Host> {
    pub fn new(host: Rc<Host>, hub: Rc<ResizeObserverHub<Host>>) -> Rc<Self> {
        let manager = Rc::new(Self {
            id: MANAGER_COUNTER.fetch_add(1, Ordering::Relaxed),
            host,
            hub,
            observed_nodes: RefCell::default(),
        });
        manager
            .hub
            .managers
            .borrow_mut()
            .insert(manager.id, Rc::downgrade(&manager));
        manager
    }

    pub fn setup(&self, pre: &NodeKey, options: ResizeManagerSetupOptions) {
        let ResizeManagerSetupOptions {
            disable_annotations,
            column_variables,
        } = options;
        let apply_column_variables = column_variables == ResizeManagerColumnVariableMode::Apply;
        let mut observed_nodes: InsertionMap<NodeKey, Observed<NodeKey>> = {
            let mut copy = InsertionMap::default();
            for (element, item) in self.observed_nodes.borrow().entries() {
                copy.insert(element, item);
            }
            copy
        };
        self.observed_nodes.borrow_mut().clear();

        let column_count =
            self.setup_code_columns(pre, apply_column_variables, &mut observed_nodes);

        if column_count > 1 && !disable_annotations {
            self.setup_annotation_pairs(pre, &mut observed_nodes);
        }

        // Cleanup any old nodes that might still be observed
        for (element, item) in observed_nodes.entries() {
            self.unobserve(&element);
            match item {
                Observed::Code(item) => cleanup_stale_code_item(self.host.as_ref(), &item),
                Observed::Annotations(item) => {
                    cleanup_stale_annotation_item(self.host.as_ref(), &item)
                }
            }
        }
        observed_nodes.clear();
    }

    // The `for (const element of pre.children)` half of `setup`: the (at most
    // two) code columns and their number gutters. Returns `columnCount`.
    fn setup_code_columns(
        &self,
        pre: &NodeKey,
        apply_column_variables: bool,
        observed_nodes: &mut InsertionMap<NodeKey, Observed<NodeKey>>,
    ) -> usize {
        let mut column_count = 0;
        for element in self.host.document().children(pre) {
            if column_count == 2 {
                break;
            }
            if self.host.document().tag_name(element) != "CODE" {
                continue;
            }
            let code_element = element;
            column_count += 1;
            let existing = observed_nodes.get(&code_element).cloned();
            let item = match existing {
                None => None,
                Some(Observed::Code(item)) => Some(item),
                Some(Observed::Annotations(_)) => panic!(
                    "ResizeManager.setup: somehow a code node is being used for an annotation, should be impossible"
                ),
            };

            let number_element = self.host.document().children(code_element).first().copied();
            if let Some(item) = item {
                self.observed_nodes
                    .borrow_mut()
                    .insert(code_element, Observed::Code(Rc::clone(&item)));
                observed_nodes.remove(&code_element);
                let previous_number_element = item.borrow().number_element;
                if previous_number_element != number_element {
                    if let Some(previous) = &previous_number_element {
                        self.unobserve(previous);
                        observed_nodes.remove(previous);
                    }
                    if let Some(number_element) = &number_element {
                        self.observe(number_element);
                        observed_nodes.remove(number_element);
                        self.observed_nodes
                            .borrow_mut()
                            .insert(*number_element, Observed::Code(Rc::clone(&item)));
                    }
                    let mut item_mut = item.borrow_mut();
                    item_mut.number_element = number_element;
                    item_mut.number_width = 0.0;
                } else if let Some(previous) = &previous_number_element {
                    observed_nodes.remove(previous);
                    self.observed_nodes
                        .borrow_mut()
                        .insert(*previous, Observed::Code(Rc::clone(&item)));
                    // If there is a resize, let the resize handler handle it...
                } else {
                    item.borrow_mut().number_width = 0.0;
                }
                sync_column_variable_mode(self.host.as_ref(), &item, apply_column_variables);
            } else {
                let item = Rc::new(RefCell::new(ObservedGridNodes {
                    code_element,
                    number_element,
                    code_width: CodeWidth::Auto,
                    number_width: 0.0,
                    apply_column_variables,
                }));
                self.observed_nodes
                    .borrow_mut()
                    .insert(code_element, Observed::Code(Rc::clone(&item)));
                self.observe(&code_element);
                if let Some(number_element) = number_element {
                    self.observed_nodes
                        .borrow_mut()
                        .insert(number_element, Observed::Code(Rc::clone(&item)));
                    self.observe(&number_element);
                }
            }
        }
        column_count
    }

    // The annotation half of `setup`: pairs of `[data-line-annotation]`
    // containers, one per column, whose children share a min-height.
    fn setup_annotation_pairs(
        &self,
        pre: &NodeKey,
        observed_nodes: &mut InsertionMap<NodeKey, Observed<NodeKey>>,
    ) {
        let mut annotation_updates: Vec<AnnotationSetup<NodeKey>> = Vec::new();
        let annotation_elements = self
            .host
            .document()
            .query_by_attribute(pre, "data-line-annotation", None)
            .into_iter()
            .filter(|element| {
                self.host
                    .document()
                    .get_attribute(*element, "data-line-annotation")
                    .is_some_and(|value| value.contains(','))
            })
            .collect::<Vec<_>>();

        let mut element_map: InsertionMap<String, Vec<NodeKey>> = InsertionMap::default();
        // Iterate through all the matched elements and organize them into pairs
        // based on the data-line-annotation attribute
        for element in annotation_elements {
            let line_annotation = self
                .host
                .document()
                .get_attribute(element, "data-line-annotation")
                .unwrap_or_default();
            if !is_line_annotation(&line_annotation) {
                self.host.report(
                    "DiffFileRenderer.setupResizeObserver: Invalid element or annotation",
                    Some(&line_annotation),
                );
                continue;
            }
            let mut pairs = element_map
                .get(&line_annotation)
                .cloned()
                .unwrap_or_default();
            pairs.push(element);
            element_map.insert(line_annotation, pairs);
        }

        for (key, pair) in element_map.entries() {
            if pair.len() != 2 {
                self.host
                    .report("DiffFileRenderer.setupResizeObserver: Bad Pair", Some(&key));
                continue;
            }
            let (container1, container2) = (pair[0], pair[1]);
            let (Some(child1), Some(child2)) = (
                self.host.document().children(container1).first().copied(),
                self.host.document().children(container2).first().copied(),
            ) else {
                continue;
            };

            let existing = observed_nodes.get(&child1).cloned();
            if let Some(item) = existing {
                self.observed_nodes
                    .borrow_mut()
                    .insert(child1, item.clone());
                self.observed_nodes.borrow_mut().insert(child2, item);
                observed_nodes.remove(&child1);
                observed_nodes.remove(&child2);
                continue;
            }

            let child1_height = self.host.bounding_height(child1);
            let child2_height = self.host.bounding_height(child2);
            let item = Rc::new(RefCell::new(ObservedAnnotationNodes {
                column1: ObservedAnnotationColumn {
                    container: container1,
                    child: child1,
                    child_height: child1_height,
                },
                column2: ObservedAnnotationColumn {
                    container: container2,
                    child: child2,
                    child_height: child2_height,
                },
                current_height: CurrentHeight::Auto,
            }));
            annotation_updates.push(AnnotationSetup {
                child1,
                child2,
                item,
                new_height: child1_height.max(child2_height),
            });
        }

        // Measure all annotation heights first, then apply the paired min-height
        // styles after the read phase so setup does not bounce between layout
        // reads and writes for every annotation pair.
        for pending_update in annotation_updates.drain(..) {
            self.apply_new_height(&pending_update.item, pending_update.new_height);
            self.observed_nodes.borrow_mut().insert(
                pending_update.child1,
                Observed::Annotations(Rc::clone(&pending_update.item)),
            );
            self.observed_nodes.borrow_mut().insert(
                pending_update.child2,
                Observed::Annotations(Rc::clone(&pending_update.item)),
            );
            self.observe(&pending_update.child1);
            self.observe(&pending_update.child2);
        }
    }

    pub fn clean_up(&self) {
        let elements: Vec<NodeKey> = self
            .observed_nodes
            .borrow()
            .entries()
            .into_iter()
            .map(|(element, _)| element)
            .collect();
        for element in elements {
            self.unobserve(&element);
        }
        self.observed_nodes.borrow_mut().clear();
    }

    fn observe(&self, element: &NodeKey) {
        let owner = self.hub.managers_by_element.borrow().get(element).copied();
        match owner {
            // Already registered
            Some(owner) if owner == self.id => return,
            // If we've already somehow registered with another manager, we in for a
            // world of pain, so complain loudly
            Some(_) => {
                panic!("ResizeManager.observe: element is already owned by another ResizeManager")
            }
            None => {}
        }
        self.hub
            .managers_by_element
            .borrow_mut()
            .insert(*element, self.id);
        self.hub.ensure_resize_observer();
        self.host.observe(*element);
    }

    fn unobserve(&self, element: &NodeKey) {
        let owner = self.hub.managers_by_element.borrow().get(element).copied();
        match owner {
            None => return,
            Some(owner) if owner != self.id => {
                panic!("ResizeManager.unobserve: element is owned by another ResizeManager")
            }
            Some(_) => {}
        }

        self.hub.managers_by_element.borrow_mut().remove(element);
        if *self.hub.observer_exists.borrow() {
            self.host.unobserve(*element);
        }
        if *self.hub.observer_exists.borrow() && self.hub.managers_by_element.borrow().is_empty() {
            self.host.disconnect_resize_observer();
            *self.hub.observer_exists.borrow_mut() = false;
        }
    }

    fn handle_resize_entries(&self, entries: &[ResizeObserverEntry<NodeKey>]) {
        let mut code_updates: InsertionMap<usize, (GridItem<NodeKey>, CodeColumnUpdate)> =
            InsertionMap::default();
        let mut annotation_updates: InsertionMap<usize, AnnotationItem<NodeKey>> =
            InsertionMap::default();
        for entry in entries {
            let target = &entry.target;
            let item = self.observed_nodes.borrow().get(target).cloned();
            let Some(item) = item else {
                self.host.report(
                    "ResizeManager.handleResizeEntries: Not a valid observed node",
                    None,
                );
                continue;
            };
            match item {
                Observed::Annotations(item) => {
                    let is_column1 = *target == item.borrow().column1.child;
                    let is_column2 = *target == item.borrow().column2.child;
                    if !is_column1 && !is_column2 {
                        self.host.report(
                            "ResizeManager.handleResizeEntries: Couldn't find a column for",
                            None,
                        );
                        continue;
                    }
                    if is_column1 {
                        item.borrow_mut().column1.child_height = entry.border_box_block_size;
                    } else {
                        item.borrow_mut().column2.child_height = entry.border_box_block_size;
                    }
                    annotation_updates.insert(item_key(&item), item);
                }
                Observed::Code(item) => {
                    let key = item_key(&item);
                    let mut update = code_updates.get(&key).map_or(
                        CodeColumnUpdate {
                            code_inline_size: None,
                            number_inline_size: None,
                        },
                        |(_, update)| *update,
                    );
                    let inline_size = entry.content_box_inline_size;
                    if *target == item.borrow().code_element {
                        update.code_inline_size = Some(inline_size);
                    } else if item.borrow().number_element.as_ref() == Some(target) {
                        update.number_inline_size = Some(inline_size);
                    }
                    code_updates.insert(key, (item, update));
                }
            }
        }
        for (_, item) in annotation_updates.entries() {
            let new_height = {
                let item = item.borrow();
                item.column1.child_height.max(item.column2.child_height)
            };
            self.apply_new_height(&item, new_height);
        }
        for (_, (item, update)) in code_updates.entries() {
            self.apply_column_update(&item, &update);
        }
    }

    fn apply_column_update(&self, item: &GridItem<NodeKey>, update: &CodeColumnUpdate) {
        let (next_code_width, next_number_width, code_width_changed, number_width_changed) = {
            let item = item.borrow();
            let next_code_width = match update.code_inline_size {
                Some(inline_size) => resolve_code_width(inline_size),
                None => item.code_width,
            };
            let next_number_width = match update.number_inline_size {
                Some(inline_size) => resolve_number_width(inline_size),
                None => item.number_width,
            };
            (
                next_code_width,
                next_number_width,
                next_code_width != item.code_width,
                next_number_width != item.number_width,
            )
        };

        if !code_width_changed && !number_width_changed {
            return;
        }

        {
            let mut item = item.borrow_mut();
            item.code_width = next_code_width;
            item.number_width = next_number_width;
        }

        if item.borrow().apply_column_variables {
            apply_code_column_variables(
                self.host.as_ref(),
                &item.borrow(),
                code_width_changed,
                number_width_changed,
            );
        }
    }

    fn apply_new_height(&self, item: &AnnotationItem<NodeKey>, new_height: f64) {
        if CurrentHeight::Px(new_height) != item.borrow().current_height {
            let current_height = new_height.max(0.0);
            item.borrow_mut().current_height = CurrentHeight::Px(current_height);
            let item = item.borrow();
            self.host.document().set_style_property(
                item.column1.container,
                "--diffs-annotation-min-height",
                &format!("{current_height}px"),
            );
            self.host.document().set_style_property(
                item.column2.container,
                "--diffs-annotation-min-height",
                &format!("{current_height}px"),
            );
        }
    }
}

// `/^-?\d+,-?\d+$/`
fn is_line_annotation(text: &str) -> bool {
    let is_signed_integer = |part: &str| {
        let digits = part.strip_prefix('-').unwrap_or(part);
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    };
    text.split_once(',')
        .is_some_and(|(left, right)| is_signed_integer(left) && is_signed_integer(right))
}

fn resolve_code_width(inline_size: f64) -> CodeWidth {
    let width = inline_size.floor().max(0.0);
    if width == 0.0 {
        CodeWidth::Auto
    } else {
        CodeWidth::Px(width)
    }
}

fn resolve_number_width(inline_size: f64) -> f64 {
    inline_size.ceil().max(0.0)
}

fn sync_column_variable_mode<Host: ResizeManagerHost>(
    host: &Host,
    item: &GridItem<NodeKey>,
    apply_column_variables: bool,
) {
    if item.borrow().apply_column_variables == apply_column_variables {
        return;
    }

    item.borrow_mut().apply_column_variables = apply_column_variables;
    if apply_column_variables {
        apply_code_column_variables(host, &item.borrow(), true, true);
    } else {
        remove_code_column_variables(host, &item.borrow());
    }
}

fn apply_code_column_variables<Host: ResizeManagerHost>(
    host: &Host,
    item: &ObservedGridNodes<NodeKey>,
    code_width_changed: bool,
    number_width_changed: bool,
) {
    let ObservedGridNodes {
        code_element,
        code_width,
        number_width,
        ..
    } = item;
    if code_width_changed {
        let value = match code_width {
            CodeWidth::Px(width) => format!("{width}px"),
            CodeWidth::Auto => "auto".to_owned(),
        };
        host.document()
            .set_style_property(code_element, "--diffs-column-width", &value);
    }

    if number_width_changed {
        let value = if *number_width == 0.0 {
            "auto".to_owned()
        } else {
            format!("{number_width}px")
        };
        host.document()
            .set_style_property(code_element, "--diffs-column-number-width", &value);
    }

    if code_width_changed || (number_width_changed && *code_width != CodeWidth::Auto) {
        let target_width = match code_width {
            CodeWidth::Px(width) => (width - number_width).max(0.0),
            CodeWidth::Auto => 0.0,
        };
        let value = if target_width > 0.0 {
            format!("{target_width}px")
        } else {
            "auto".to_owned()
        };
        host.document()
            .set_style_property(code_element, "--diffs-column-content-width", &value);
    }
}

fn remove_code_column_variables<Host: ResizeManagerHost>(
    host: &Host,
    item: &ObservedGridNodes<NodeKey>,
) {
    host.document()
        .remove_style_property(item.code_element, "--diffs-column-content-width");
    host.document()
        .remove_style_property(item.code_element, "--diffs-column-number-width");
    host.document()
        .remove_style_property(item.code_element, "--diffs-column-width");
}

fn cleanup_stale_code_item<Host: ResizeManagerHost>(host: &Host, item: &GridItem<NodeKey>) {
    let item = item.borrow();
    if host.is_connected(item.code_element) {
        remove_code_column_variables(host, &item);
    }
}

fn cleanup_stale_annotation_item<Host: ResizeManagerHost>(
    host: &Host,
    item: &AnnotationItem<NodeKey>,
) {
    let item = item.borrow();
    if host.is_connected(item.column1.container) {
        host.document()
            .remove_style_property(item.column1.container, "--diffs-annotation-min-height");
    }
    if host.is_connected(item.column2.container) {
        host.document()
            .remove_style_property(item.column2.container, "--diffs-annotation-min-height");
    }
}
