// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/InteractionManager.gutterUtility.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; jsdom is the
// `FakeDom` model (`fake_interaction_dom.rs`) and `dispatchPointer` is
// `Harness::dispatch`, which routes a pointer event the way bubbling does: the
// `pre` listeners first (`click`, `pointerdown`, `pointermove`,
// `pointerleave`), then the document listeners (`pointermove`, `pointerup`,
// `pointercancel`), and returns whether either handler prevented the default;
// `setElementFromPoint` writes the fake's hit table; `querySelector` checks are
// `has_descendant`. The `InteractionManager editor active-line state` describe
// block (six tests) is not ported: the editor active line is dropped. Tests
// whose callbacks re-enter the manager hold it through a `Weak`, as the
// upstream closures hold the `const manager`.

use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;

use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::managers::interaction_manager::GetHoveredLineResult;
use crate::managers::interaction_manager::InteractionManager;
use crate::managers::interaction_manager::InteractionManagerMode;
use crate::managers::interaction_manager::InteractionManagerOptions;
use crate::managers::interaction_manager::PointerEventData;
use crate::managers::interaction_manager::PointerType;
use crate::managers::interaction_manager::SelectionCallback;
use crate::managers::interaction_manager::SelectionWriteOptions;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::view::AnnotationSide;
use crate::types::view::SelectedLineRange;

type Events = Rc<RefCell<Vec<(&'static str, Option<SelectedLineRange>)>>>;

struct FilePreFixture {
    content_rows: Vec<NodeKey>,
    gutter_rows: Vec<NodeKey>,
    pre: NodeKey,
    gutter: NodeKey,
    content: NodeKey,
}

struct Pointer {
    pointer_id: i32,
    pointer_type: PointerType,
    client_x: f64,
    client_y: f64,
}

impl Pointer {
    fn new(pointer_type: PointerType) -> Self {
        Self {
            pointer_id: 1,
            pointer_type,
            client_x: f64::NAN,
            client_y: f64::NAN,
        }
    }

    fn id(mut self, pointer_id: i32) -> Self {
        self.pointer_id = pointer_id;
        self
    }

    fn at(mut self, client_x: f64, client_y: f64) -> Self {
        self.client_x = client_x;
        self.client_y = client_y;
        self
    }
}

struct Harness {
    dom: Rc<FakeDom>,
    manager: Rc<InteractionManager<FakeDom>>,
}

impl Harness {
    fn new(mode: InteractionManagerMode, options: InteractionManagerOptions<NodeKey>) -> Self {
        let dom = Rc::new(FakeDom::default());
        dom.hit_test_available.set(true);
        let manager = InteractionManager::new(
            Rc::clone(&dom),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            mode,
            options,
        );
        Self { dom, manager }
    }

    fn create(&self, tag: &str, attrs: &[(&str, &str)]) -> NodeKey {
        let id = self.dom.doc.create_element(tag);
        for (name, value) in attrs {
            self.dom.doc.set_attribute(id, name, value);
        }
        id
    }

    fn file_pre(&self, line_count: usize) -> FilePreFixture {
        let pre = self.create("pre", &[]);
        let code = self.create("div", &[("data-code", "")]);
        let gutter = self.create("div", &[("data-gutter", "")]);
        let content = self.create("div", &[("data-content", "")]);
        let mut gutter_rows = Vec::new();
        let mut content_rows = Vec::new();
        for index in 0..line_count {
            let line_number = (index + 1).to_string();
            let index = index.to_string();
            let gutter_row = self.create(
                "div",
                &[
                    ("data-column-number", &line_number),
                    ("data-line-index", &index),
                    ("data-line-type", "context"),
                ],
            );
            gutter_rows.push(gutter_row);
            self.dom.doc.append_child(gutter, gutter_row);
            let content_row = self.create(
                "div",
                &[
                    ("data-line", &line_number),
                    ("data-line-index", &index),
                    ("data-line-type", "context"),
                ],
            );
            content_rows.push(content_row);
            self.dom.doc.append_child(content, content_row);
        }
        self.dom.doc.append_child(code, gutter);
        self.dom.doc.append_child(code, content);
        self.dom.doc.append_child(pre, code);
        FilePreFixture {
            content_rows,
            gutter_rows,
            pre,
            gutter,
            content,
        }
    }

    // `element.after(sibling)`
    fn insert_after(&self, reference: NodeKey, sibling: NodeKey) {
        let parent = self.dom.doc.parent(reference).expect("a parent");
        let next = self
            .dom
            .doc
            .children(parent)
            .into_iter()
            .skip_while(|key| *key != reference)
            .nth(1);
        self.dom.doc.insert_before(parent, sibling, next);
    }

    fn insert_first(&self, parent: NodeKey, child: NodeKey) {
        let first = self.dom.doc.children(parent).first().copied();
        self.dom.doc.insert_before(parent, child, first);
    }

    fn set_element_from_point(&self, x: f64, y: f64, element: NodeKey) {
        self.dom
            .hits
            .borrow_mut()
            .insert(format!("{x},{y}"), Some(element));
    }

    fn has_descendant(&self, root: NodeKey, attribute: &str) -> bool {
        !self
            .dom
            .doc
            .query_by_attribute(root, attribute, None)
            .is_empty()
    }

    fn utility_button(&self, row: NodeKey) -> NodeKey {
        *self
            .dom
            .doc
            .query_by_attribute(row, "data-utility-button", None)
            .first()
            .expect("missing gutter utility button")
    }

    fn dispatch(&self, target: NodeKey, event_type: &str, pointer: &Pointer) -> bool {
        let event = PointerEventData {
            pointer_id: pointer.pointer_id,
            pointer_type: pointer.pointer_type,
            button: 0,
            shift_key: false,
            client_x: pointer.client_x,
            client_y: pointer.client_y,
            target: Some(target),
        };
        let mut prevented = false;
        if self.dom.pre_listener.get() {
            let response = match event_type {
                "pointerdown" => Some(self.manager.handle_pointer_down(&event)),
                "pointermove" => Some(self.manager.handle_pointer_move(&event)),
                _ => None,
            };
            if let Some(response) = response {
                prevented |= response.prevent_default;
                if response.stop_propagation {
                    return prevented;
                }
            }
        }
        if self.dom.document_listener.get() {
            let response = match event_type {
                "pointermove" => Some(self.manager.handle_document_pointer_move(&event)),
                "pointerup" => Some(self.manager.handle_document_pointer_up(&event)),
                _ => None,
            };
            if let Some(response) = response {
                prevented |= response.prevent_default;
            }
        }
        prevented
    }
}

fn range(start: usize, end: usize) -> SelectedLineRange {
    SelectedLineRange {
        start,
        end,
        side: None,
        end_side: None,
    }
}

fn selection_recorder(events: &Events, name: &'static str) -> SelectionCallback {
    let events = Rc::clone(events);
    Rc::new(move |range: Option<&SelectedLineRange>| {
        events.borrow_mut().push((name, range.cloned()));
    })
}

fn options_with_utility() -> InteractionManagerOptions<NodeKey> {
    let mut options = InteractionManagerOptions::default();
    options.base.enable_gutter_utility = Some(true);
    options
}

#[test]
fn does_not_reveal_the_gutter_utility_while_touch_dragging_from_content() {
    let harness = Harness::new(InteractionManagerMode::File, options_with_utility());
    let fixture = harness.file_pre(3);
    harness.manager.setup(&fixture.pre);

    harness.dispatch(
        fixture.content_rows[1],
        "pointerdown",
        &Pointer::new(PointerType::Touch),
    );
    harness.dispatch(
        fixture.content_rows[1],
        "pointermove",
        &Pointer::new(PointerType::Touch),
    );

    assert!(!harness.has_descendant(fixture.pre, "data-gutter-utility-slot"));
    assert_eq!(harness.manager.get_hovered_line(), None);
    harness.manager.clean_up();
}

#[test]
fn reveals_the_gutter_utility_after_a_touch_tap_on_a_gutter_row() {
    let harness = Harness::new(InteractionManagerMode::File, options_with_utility());
    let fixture = harness.file_pre(3);
    harness.manager.setup(&fixture.pre);

    harness.dispatch(
        fixture.gutter_rows[2],
        "pointerdown",
        &Pointer::new(PointerType::Touch),
    );

    assert!(harness.has_descendant(fixture.gutter_rows[2], "data-gutter-utility-slot"));
    assert_eq!(
        harness.manager.get_hovered_line(),
        Some(GetHoveredLineResult {
            line_number: 3,
            side: None
        })
    );
    harness.manager.clean_up();
}

#[test]
fn does_not_reveal_the_gutter_utility_from_mouse_down_alone() {
    let harness = Harness::new(InteractionManagerMode::File, options_with_utility());
    let fixture = harness.file_pre(3);
    harness.manager.setup(&fixture.pre);

    harness.dispatch(
        fixture.content_rows[1],
        "pointerdown",
        &Pointer::new(PointerType::Mouse),
    );

    assert!(!harness.has_descendant(fixture.pre, "data-gutter-utility-slot"));
    assert_eq!(harness.manager.get_hovered_line(), None);
    harness.manager.clean_up();
}

#[test]
fn anchors_selected_gutter_utility_to_the_bottom_most_selected_row() {
    let harness = Harness::new(InteractionManagerMode::File, options_with_utility());
    let fixture = harness.file_pre(4);
    harness.manager.setup(&fixture.pre);

    harness
        .manager
        .set_selection(Some(range(3, 1)), SelectionWriteOptions::default());

    assert!(harness.has_descendant(fixture.gutter_rows[2], "data-gutter-utility-slot"));
    assert_eq!(
        harness.manager.get_hovered_line(),
        Some(GetHoveredLineResult {
            line_number: 3,
            side: None
        })
    );
    harness.manager.clean_up();
}

#[test]
fn pressing_the_selected_gutter_utility_uses_the_whole_selection() {
    let clicked: Rc<RefCell<Vec<SelectedLineRange>>> = Rc::default();
    let mut options = options_with_utility();
    let recorder = Rc::clone(&clicked);
    options.base.on_gutter_utility_click =
        Some(Rc::new(move |range| recorder.borrow_mut().push(range)));
    let harness = Harness::new(InteractionManagerMode::File, options);
    let fixture = harness.file_pre(4);
    harness.manager.setup(&fixture.pre);
    harness
        .manager
        .set_selection(Some(range(3, 1)), SelectionWriteOptions::default());
    let button = harness.utility_button(fixture.gutter_rows[2]);

    let touch = Pointer::new(PointerType::Touch).id(7);
    harness.dispatch(button, "pointerdown", &touch);
    harness.dispatch(button, "pointerup", &touch);

    assert_eq!(*clicked.borrow(), vec![range(1, 3)]);
    harness.manager.clean_up();
}

#[test]
fn dragging_the_gutter_utility_selects_and_notifies_when_line_selection_is_disabled() {
    let events: Events = Rc::default();
    let mut options = options_with_utility();
    options.base.enable_line_selection = Some(false);
    let utility_events = Rc::clone(&events);
    options.base.on_gutter_utility_click = Some(Rc::new(move |range| {
        utility_events.borrow_mut().push(("utility", Some(range)));
    }));
    options.base.on_line_selected = Some(selection_recorder(&events, "selected"));
    options.base.on_line_selection_start = Some(selection_recorder(&events, "start"));
    options.base.on_line_selection_change = Some(selection_recorder(&events, "change"));
    options.base.on_line_selection_end = Some(selection_recorder(&events, "end"));
    let harness = Harness::new(InteractionManagerMode::File, options);
    let fixture = harness.file_pre(4);
    harness.manager.setup(&fixture.pre);
    harness.dispatch(
        fixture.gutter_rows[1],
        "pointermove",
        &Pointer::new(PointerType::Mouse),
    );
    let button = harness.utility_button(fixture.gutter_rows[1]);
    harness.set_element_from_point(8.0, 80.0, fixture.gutter_rows[3]);

    let at = |y| Pointer::new(PointerType::Mouse).id(19).at(8.0, y);
    harness.dispatch(button, "pointerdown", &at(40.0));

    assert_eq!(harness.manager.get_selection(), Some(range(2, 2)));
    assert_eq!(*events.borrow(), vec![("start", Some(range(2, 2)))]);

    harness.dispatch(button, "pointermove", &at(80.0));

    assert_eq!(harness.manager.get_selection(), Some(range(2, 4)));
    assert_eq!(
        *events.borrow(),
        vec![("start", Some(range(2, 2))), ("change", Some(range(2, 4))),]
    );

    harness.dispatch(button, "pointerup", &at(80.0));

    assert_eq!(harness.manager.get_selection(), Some(range(2, 4)));
    assert_eq!(
        *events.borrow(),
        vec![
            ("start", Some(range(2, 2))),
            ("change", Some(range(2, 4))),
            ("utility", Some(range(2, 4))),
            ("end", Some(range(2, 4))),
            ("selected", Some(range(2, 4))),
        ]
    );
    harness.manager.clean_up();
}

#[test]
fn keeps_the_completed_gutter_gesture_after_its_utility_callback_clears_selection() {
    let events: Events = Rc::default();
    let manager_slot: Rc<RefCell<Weak<InteractionManager<FakeDom>>>> = Rc::default();
    let mut options = options_with_utility();
    options.base.enable_line_selection = Some(false);
    let utility_events = Rc::clone(&events);
    let utility_manager = Rc::clone(&manager_slot);
    options.base.on_gutter_utility_click = Some(Rc::new(move |range| {
        utility_events.borrow_mut().push(("utility", Some(range)));
        utility_manager
            .borrow()
            .upgrade()
            .expect("the manager is alive")
            .set_selection(
                None,
                SelectionWriteOptions {
                    notify: Some(false),
                    ..SelectionWriteOptions::default()
                },
            );
    }));
    options.base.on_line_selected = Some(selection_recorder(&events, "selected"));
    options.base.on_line_selection_end = Some(selection_recorder(&events, "end"));
    let harness = Harness::new(InteractionManagerMode::File, options);
    *manager_slot.borrow_mut() = Rc::downgrade(&harness.manager);
    let fixture = harness.file_pre(4);
    harness.manager.setup(&fixture.pre);
    harness.dispatch(
        fixture.gutter_rows[1],
        "pointermove",
        &Pointer::new(PointerType::Mouse),
    );
    let button = harness.utility_button(fixture.gutter_rows[1]);
    harness.set_element_from_point(8.0, 80.0, fixture.gutter_rows[3]);

    let at = |y| Pointer::new(PointerType::Mouse).id(21).at(8.0, y);
    harness.dispatch(button, "pointerdown", &at(40.0));
    harness.dispatch(button, "pointermove", &at(80.0));
    harness.dispatch(button, "pointerup", &at(80.0));

    let completed = range(2, 4);
    assert_eq!(
        *events.borrow(),
        vec![
            ("utility", Some(completed.clone())),
            ("end", Some(completed.clone())),
            ("selected", Some(completed)),
        ]
    );
    assert_eq!(harness.manager.get_selection(), None);
    harness.manager.clean_up();
}

#[test]
fn keeps_a_controlled_cross_side_gesture_after_its_utility_callback_replaces_selection() {
    let events: Events = Rc::default();
    let manager_slot: Rc<RefCell<Weak<InteractionManager<FakeDom>>>> = Rc::default();
    let mut options = InteractionManagerOptions::default();
    options.base.controlled_selection = Some(true);
    options.base.enable_gutter_utility = Some(true);
    options.base.enable_line_selection = Some(false);
    let utility_events = Rc::clone(&events);
    let utility_manager = Rc::clone(&manager_slot);
    options.base.on_gutter_utility_click = Some(Rc::new(move |range| {
        utility_events.borrow_mut().push(("utility", Some(range)));
        utility_manager
            .borrow()
            .upgrade()
            .expect("the manager is alive")
            .set_selection(
                Some(SelectedLineRange {
                    start: 2,
                    end: 2,
                    side: Some(AnnotationSide::Additions),
                    end_side: None,
                }),
                SelectionWriteOptions {
                    notify: Some(false),
                    ..SelectionWriteOptions::default()
                },
            );
    }));
    options.base.on_line_selected = Some(selection_recorder(&events, "selected"));
    options.base.on_line_selection_end = Some(selection_recorder(&events, "end"));
    let harness = Harness::new(InteractionManagerMode::Diff, options);
    *manager_slot.borrow_mut() = Rc::downgrade(&harness.manager);
    let fixture = harness.file_pre(2);
    for row in [fixture.content_rows[0], fixture.gutter_rows[0]] {
        harness
            .dom
            .doc
            .set_attribute(row, "data-line-type", "change-deletion");
        harness.dom.doc.set_attribute(row, "data-line-index", "0,0");
    }
    for row in [fixture.content_rows[1], fixture.gutter_rows[1]] {
        harness
            .dom
            .doc
            .set_attribute(row, "data-line-type", "change-addition");
        harness.dom.doc.set_attribute(row, "data-line-index", "1,1");
    }
    harness.manager.setup(&fixture.pre);
    harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Mouse),
    );
    let button = harness.utility_button(fixture.gutter_rows[0]);
    harness.set_element_from_point(8.0, 40.0, fixture.gutter_rows[1]);

    let at = |y| Pointer::new(PointerType::Mouse).id(22).at(8.0, y);
    harness.dispatch(button, "pointerdown", &at(20.0));
    harness.dispatch(button, "pointermove", &at(40.0));
    harness.dispatch(button, "pointerup", &at(40.0));

    let completed = SelectedLineRange {
        start: 1,
        end: 2,
        side: Some(AnnotationSide::Deletions),
        end_side: Some(AnnotationSide::Additions),
    };
    assert_eq!(
        *events.borrow(),
        vec![
            ("utility", Some(completed.clone())),
            ("end", Some(completed.clone())),
            ("selected", Some(completed)),
        ]
    );
    assert_eq!(
        harness.manager.get_selection(),
        Some(SelectedLineRange {
            start: 2,
            end: 2,
            side: Some(AnnotationSide::Additions),
            end_side: None,
        })
    );
    harness.manager.clean_up();
}

#[test]
fn keeps_ordinary_gutter_selection_disabled_when_only_the_utility_can_select() {
    let events: Events = Rc::default();
    let mut options = options_with_utility();
    options.base.enable_line_selection = Some(false);
    let utility_events = Rc::clone(&events);
    options.base.on_gutter_utility_click = Some(Rc::new(move |range| {
        utility_events.borrow_mut().push(("utility", Some(range)));
    }));
    options.base.on_line_selected = Some(selection_recorder(&events, "selected"));
    options.base.on_line_selection_start = Some(selection_recorder(&events, "start"));
    options.base.on_line_selection_change = Some(selection_recorder(&events, "change"));
    options.base.on_line_selection_end = Some(selection_recorder(&events, "end"));
    let harness = Harness::new(InteractionManagerMode::File, options);
    let fixture = harness.file_pre(4);
    harness.manager.setup(&fixture.pre);
    harness.set_element_from_point(8.0, 80.0, fixture.gutter_rows[3]);

    let at = |y| Pointer::new(PointerType::Mouse).id(20).at(8.0, y);
    harness.dispatch(fixture.gutter_rows[0], "pointerdown", &at(20.0));
    harness.dispatch(fixture.gutter_rows[0], "pointermove", &at(80.0));
    harness.dispatch(fixture.gutter_rows[0], "pointerup", &at(80.0));

    assert_eq!(harness.manager.get_selection(), None);
    assert!(events.borrow().is_empty());
    harness.manager.clean_up();
}

#[test]
fn dragging_the_selected_gutter_utility_extends_selection_on_touch() {
    let clicked: Rc<RefCell<Vec<SelectedLineRange>>> = Rc::default();
    let mut options = options_with_utility();
    options.base.enable_line_selection = Some(true);
    let recorder = Rc::clone(&clicked);
    options.base.on_gutter_utility_click =
        Some(Rc::new(move |range| recorder.borrow_mut().push(range)));
    let harness = Harness::new(InteractionManagerMode::File, options);
    let fixture = harness.file_pre(4);
    harness.manager.setup(&fixture.pre);
    harness
        .manager
        .set_selection(Some(range(1, 2)), SelectionWriteOptions::default());
    let button = harness.utility_button(fixture.gutter_rows[1]);
    harness.set_element_from_point(8.0, 80.0, fixture.gutter_rows[3]);

    let at = |y| Pointer::new(PointerType::Touch).id(9).at(8.0, y);
    let pointer_down_prevented = harness.dispatch(button, "pointerdown", &at(40.0));
    let pointer_move_prevented = harness.dispatch(button, "pointermove", &at(80.0));
    harness.dispatch(button, "pointerup", &at(80.0));

    assert!(pointer_down_prevented);
    assert!(pointer_move_prevented);
    assert_eq!(harness.manager.get_selection(), Some(range(1, 4)));
    assert_eq!(*clicked.borrow(), vec![range(1, 4)]);
    assert!(harness.has_descendant(fixture.gutter_rows[3], "data-gutter-utility-slot"));
    harness.manager.clean_up();
}

fn line_selection_options() -> InteractionManagerOptions<NodeKey> {
    let mut options = options_with_utility();
    options.base.enable_line_selection = Some(true);
    options
}

#[test]
fn normal_touch_line_selection_follows_coordinates_when_the_pointer_target_is_captured() {
    let harness = Harness::new(InteractionManagerMode::File, line_selection_options());
    let fixture = harness.file_pre(4);
    harness.manager.setup(&fixture.pre);
    harness.set_element_from_point(8.0, 80.0, fixture.content_rows[3]);

    let at = |y| Pointer::new(PointerType::Touch).id(11).at(8.0, y);
    harness.dispatch(fixture.gutter_rows[0], "pointerdown", &at(20.0));
    let pointer_move_prevented = harness.dispatch(fixture.gutter_rows[0], "pointermove", &at(80.0));
    harness.dispatch(fixture.gutter_rows[0], "pointerup", &at(80.0));

    assert!(pointer_move_prevented);
    assert_eq!(harness.manager.get_selection(), Some(range(1, 4)));
    assert!(harness.has_descendant(fixture.gutter_rows[3], "data-gutter-utility-slot"));
    harness.manager.clean_up();
}

#[test]
fn normal_touch_line_selection_normalizes_lateral_hits_to_selectable_rows() {
    let harness = Harness::new(InteractionManagerMode::File, line_selection_options());
    let fixture = harness.file_pre(4);
    let token = harness.create("span", &[("data-char", "0")]);
    harness
        .dom
        .doc
        .replace_children(fixture.content_rows[3], &[token]);
    let line_number = harness.create("span", &[("data-line-number-content", "")]);
    harness
        .dom
        .doc
        .append_child(fixture.gutter_rows[2], line_number);
    harness.manager.setup(&fixture.pre);
    harness.set_element_from_point(80.0, 80.0, token);
    harness.set_element_from_point(4.0, 60.0, line_number);

    harness.dispatch(
        fixture.gutter_rows[0],
        "pointerdown",
        &Pointer::new(PointerType::Touch).id(12).at(8.0, 20.0),
    );
    harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(12).at(80.0, 80.0),
    );

    assert_eq!(harness.manager.get_selection(), Some(range(1, 4)));

    harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(12).at(4.0, 60.0),
    );

    assert_eq!(harness.manager.get_selection(), Some(range(1, 3)));
    harness.manager.clean_up();
}

// `createAnnotationRowAfter`: a gutter buffer and an annotation row after the
// rows of one line; returns the annotation content element.
fn create_annotation_row_after(
    harness: &Harness,
    fixture: &FilePreFixture,
    line_index: usize,
) -> NodeKey {
    let gutter_annotation = harness.create(
        "div",
        &[
            ("data-gutter-buffer", "annotation"),
            ("data-buffer-size", "1"),
        ],
    );
    let content_annotation = harness.create(
        "div",
        &[("data-line-annotation", &format!("0,{line_index}"))],
    );
    let annotation_content = harness.create("div", &[("data-annotation-content", "")]);
    harness
        .dom
        .doc
        .append_child(content_annotation, annotation_content);
    harness.insert_after(fixture.gutter_rows[line_index], gutter_annotation);
    harness.insert_after(fixture.content_rows[line_index], content_annotation);
    annotation_content
}

#[test]
fn normal_touch_line_selection_follows_annotation_rows() {
    let harness = Harness::new(InteractionManagerMode::File, line_selection_options());
    let fixture = harness.file_pre(4);
    let annotation_content = create_annotation_row_after(&harness, &fixture, 2);
    harness.manager.setup(&fixture.pre);
    harness.set_element_from_point(80.0, 60.0, annotation_content);

    harness.dispatch(
        fixture.gutter_rows[0],
        "pointerdown",
        &Pointer::new(PointerType::Touch).id(16).at(8.0, 20.0),
    );
    let pointer_move_prevented = harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(16).at(80.0, 60.0),
    );

    assert!(pointer_move_prevented);
    assert_eq!(harness.manager.get_selection(), Some(range(1, 3)));
    assert!(harness.has_descendant(fixture.gutter_rows[2], "data-gutter-utility-slot"));
    harness.manager.clean_up();
}

// A slotted annotation: light-DOM content with a `slot` attribute, outside the
// `pre` (jsdom assigns no slot, so the slot name is what the manager reads).
fn slotted_annotation_button(harness: &Harness, slot: &str) -> NodeKey {
    let content = harness.create("div", &[("slot", slot)]);
    let button = harness.create("button", &[("type", "button")]);
    harness.dom.doc.append_child(content, button);
    button
}

#[test]
fn normal_touch_line_selection_follows_slotted_annotation_content() {
    let harness = Harness::new(InteractionManagerMode::File, line_selection_options());
    let fixture = harness.file_pre(4);
    let button = slotted_annotation_button(&harness, "annotation-3");
    harness.manager.setup(&fixture.pre);
    harness.set_element_from_point(80.0, 60.0, button);

    harness.dispatch(
        fixture.gutter_rows[0],
        "pointerdown",
        &Pointer::new(PointerType::Touch).id(17).at(8.0, 20.0),
    );
    let pointer_move_prevented = harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(17).at(80.0, 60.0),
    );

    assert!(pointer_move_prevented);
    assert_eq!(harness.manager.get_selection(), Some(range(1, 3)));
    assert!(harness.has_descendant(fixture.gutter_rows[2], "data-gutter-utility-slot"));
    harness.manager.clean_up();
}

#[test]
fn normal_touch_line_selection_ignores_slotted_file_level_annotation_content() {
    let harness = Harness::new(InteractionManagerMode::File, line_selection_options());
    let fixture = harness.file_pre(4);
    // `prependLineZeroRow`
    let gutter_row = harness.create(
        "div",
        &[
            ("data-column-number", "0"),
            ("data-line-index", "-1"),
            ("data-line-type", "context"),
        ],
    );
    let content_row = harness.create(
        "div",
        &[
            ("data-line", "0"),
            ("data-line-index", "-1"),
            ("data-line-type", "context"),
        ],
    );
    harness.insert_first(fixture.gutter, gutter_row);
    harness.insert_first(fixture.content, content_row);
    let button = slotted_annotation_button(&harness, "annotation-0");
    harness.manager.setup(&fixture.pre);
    harness.set_element_from_point(8.0, 80.0, fixture.gutter_rows[3]);
    harness.set_element_from_point(80.0, 60.0, button);

    harness.dispatch(
        fixture.gutter_rows[0],
        "pointerdown",
        &Pointer::new(PointerType::Touch).id(18).at(8.0, 20.0),
    );
    harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(18).at(8.0, 80.0),
    );
    assert_eq!(harness.manager.get_selection(), Some(range(1, 4)));

    harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(18).at(80.0, 60.0),
    );

    assert_eq!(harness.manager.get_selection(), Some(range(1, 4)));
    harness.manager.clean_up();
}

#[test]
fn normal_touch_line_selection_holds_range_while_dragging_over_hunk_separators() {
    let harness = Harness::new(InteractionManagerMode::File, line_selection_options());
    let fixture = harness.file_pre(4);
    let separator = harness.create(
        "div",
        &[("data-expand-index", "0"), ("data-expand-button", "")],
    );
    harness.dom.doc.append_child(fixture.pre, separator);
    harness.manager.setup(&fixture.pre);
    harness.set_element_from_point(8.0, 80.0, fixture.gutter_rows[3]);
    harness.set_element_from_point(8.0, 48.0, separator);

    harness.dispatch(
        fixture.gutter_rows[0],
        "pointerdown",
        &Pointer::new(PointerType::Touch).id(13).at(8.0, 20.0),
    );
    harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(13).at(8.0, 80.0),
    );
    assert_eq!(harness.manager.get_selection(), Some(range(1, 4)));

    harness.dispatch(
        fixture.gutter_rows[0],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(13).at(8.0, 48.0),
    );

    assert_eq!(harness.manager.get_selection(), Some(range(1, 4)));
    harness.manager.clean_up();
}

#[test]
fn normal_touch_line_selection_holds_range_while_dragging_over_the_gutter_utility() {
    let harness = Harness::new(InteractionManagerMode::File, line_selection_options());
    let fixture = harness.file_pre(4);
    harness.manager.setup(&fixture.pre);
    harness.set_element_from_point(8.0, 20.0, fixture.gutter_rows[0]);

    harness.dispatch(
        fixture.gutter_rows[3],
        "pointerdown",
        &Pointer::new(PointerType::Touch).id(15).at(8.0, 80.0),
    );
    harness.dispatch(
        fixture.gutter_rows[3],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(15).at(8.0, 20.0),
    );
    assert_eq!(harness.manager.get_selection(), Some(range(4, 1)));

    let button = harness.utility_button(fixture.gutter_rows[3]);
    harness.set_element_from_point(8.0, 80.0, button);
    harness.dispatch(
        fixture.gutter_rows[3],
        "pointermove",
        &Pointer::new(PointerType::Touch).id(15).at(8.0, 80.0),
    );

    assert_eq!(harness.manager.get_selection(), Some(range(4, 1)));
    harness.manager.clean_up();
}

// Not in upstream's file. A render replaces the rows the pointer was over; the owner runs the
// manager's `setup` for the new rows and only then frees the replaced ones. Upstream's hovered
// line and utility anchor would be detached elements by then; here `setup` ends them while the
// rows exist, so freeing the rows leaves no stale key behind (see `forget_replaced_rows`).
#[test]
fn setup_forgets_the_hovered_line_and_utility_anchor_of_rows_a_render_replaced() {
    let mut options = options_with_utility();
    options.base.line_hover_highlight =
        Some(crate::managers::interaction_manager::LineHoverHighlight::Both);
    let harness = Harness::new(InteractionManagerMode::File, options);
    let fixture = harness.file_pre(3);
    harness.manager.setup(&fixture.pre);
    harness.dispatch(
        fixture.content_rows[1],
        "pointermove",
        &Pointer::new(PointerType::Mouse),
    );
    assert!(harness.manager.get_hovered_line().is_some());
    assert!(harness.has_descendant(fixture.gutter_rows[1], "data-gutter-utility-slot"));

    // The render: the rows are replaced, `setup` runs, then the old rows are freed.
    for row in [fixture.content_rows[1], fixture.gutter_rows[1]] {
        harness.dom.doc.remove(row);
    }
    harness.manager.setup(&fixture.pre);
    assert_eq!(harness.manager.get_hovered_line(), None);
    assert!(!harness.has_descendant(fixture.pre, "data-gutter-utility-slot"));
    for row in [fixture.content_rows[1], fixture.gutter_rows[1]] {
        harness.dom.doc.discard(row);
    }

    // The manager still owns the utility: it places it on the next hover, and cleans up.
    harness.dispatch(
        fixture.content_rows[2],
        "pointermove",
        &Pointer::new(PointerType::Mouse),
    );
    assert!(harness.has_descendant(fixture.gutter_rows[2], "data-gutter-utility-slot"));
    harness.manager.clean_up();
}
