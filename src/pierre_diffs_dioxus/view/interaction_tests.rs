//! The view's pointer interaction in a headless `VirtualDom`.
//!
//! No engine is here, so the tests play the platform's part: a pointer event is a serialized
//! payload handed to the very handlers the renderer attached (`Pointers::listeners`), delivered in
//! bubbling order from the node it hit up to the view's top node, with the one event instance a
//! browser shares between them. What each test reads back is the document the manager edited
//! (`data-hovered`, `data-selected-line`, the rows after an expansion) and what the caller's
//! callback was told. Geometry (client rects for the drag's hit-testing) is scripted.

use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use dioxus::core::AttributeValue;
use dioxus::core::Event;
use dioxus::core::Mutation;
use dioxus::html::MountedData;
use dioxus::html::MountedResult;
use dioxus::html::RenderedElementBacking;
use dioxus::html::geometry::PixelsRect;
use dioxus::html::geometry::euclid::Point2D;
use dioxus::html::geometry::euclid::Size2D;
use dioxus_html::EventData;
use dioxus_html::SerializedMouseData;
use dioxus_html::SerializedPointInteraction;
use dioxus_html::SerializedPointerData;
use futures::FutureExt;
use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;
use pierre_diffs::types::patch::FileContents;
use pierre_diffs::types::view::SelectedLineRange;

use super::*;

/// The platform's client rects: a scripted element answers `get_client_rect`.
struct Rect(PixelsRect);

impl RenderedElementBacking for Rect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_client_rect(&self) -> Pin<Box<dyn Future<Output = MountedResult<PixelsRect>>>> {
        let rect = self.0;
        Box::pin(async move { Ok(rect) })
    }
}

fn rect(x: f64, y: f64, width: f64, height: f64) -> Rect {
    Rect(PixelsRect::new(
        Point2D::new(x, y),
        Size2D::new(width, height),
    ))
}

/// What the caller's callback was told, and the view the page rendered.
#[derive(Clone, Default)]
struct Recorder {
    selected: Rc<RefCell<Vec<Option<SelectedLineRange>>>>,
    view: Rc<RefCell<Option<Rc<View<()>>>>>,
}

impl PartialEq for Recorder {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

#[derive(Props, Clone, PartialEq)]
struct PageProps {
    recorder: Recorder,
    input: DiffInput,
    virtualized: bool,
    expand_unchanged: bool,
    interaction: bool,
}

#[component]
fn Page(props: PageProps) -> Element {
    let selected = Rc::clone(&props.recorder.selected);
    let never: Rc<dyn Fn(Duration) -> SleepFuture> = Rc::new(|_| Box::pin(std::future::pending()));
    let view = use_view(&FileDiffViewProps::<()> {
        input: props.input.clone(),
        diff_style: DiffStyle::Unified,
        expand_unchanged: props.expand_unchanged,
        annotations: Vec::new(),
        render_annotation: None,
        highlighter: None,
        virtualized: props.virtualized.then(|| Virtualization {
            height: "600px".to_owned(),
            sleep: PtrEq::new(never),
        }),
        interaction: if props.interaction {
            Interaction {
                line_hover_highlight: LineHoverHighlight::Both,
                enable_line_selection: true,
                on_line_selected: Some(Callback::new(move |range| {
                    selected.borrow_mut().push(range);
                })),
                ..Interaction::default()
            }
        } else {
            Interaction::default()
        },
    });
    *props.recorder.view.borrow_mut() = Some(Rc::clone(&view));
    view.element()
}

fn file(contents: &str) -> Box<FileContents> {
    Box::new(FileContents {
        name: "a.rs".to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    })
}

/// `count` numbered lines, the ones in `changed` rewritten.
fn pair(count: usize, changed: &[usize]) -> DiffInput {
    let lines = |edit: bool| {
        (1..=count)
            .map(|number| {
                if edit && changed.contains(&number) {
                    format!("let line_{number} = changed;")
                } else {
                    format!("let line_{number} = {number};")
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    };
    DiffInput::Files {
        old_file: file(&lines(false)),
        new_file: file(&lines(true)),
    }
}

/// A pointer event as the platform delivers it.
#[derive(Clone, Copy)]
struct Pointer {
    id: i32,
    kind: &'static str,
    x: f64,
    y: f64,
    /// The button that changed state (`-1` for none), and the buttons held.
    button: i16,
    buttons: u16,
    shift: bool,
}

impl Pointer {
    fn mouse(x: f64, y: f64) -> Self {
        Self {
            id: 1,
            kind: "mouse",
            x,
            y,
            button: -1,
            buttons: 0,
            shift: false,
        }
    }

    fn pressed(self) -> Self {
        Self {
            button: 0,
            buttons: 1,
            ..self
        }
    }

    fn dragging(self) -> Self {
        Self { buttons: 1, ..self }
    }

    fn shift(self) -> Self {
        Self {
            shift: true,
            ..self
        }
    }

    fn point(self) -> SerializedPointInteraction {
        SerializedPointInteraction {
            client_x: self.x,
            client_y: self.y,
            button: self.button,
            buttons: self.buttons,
            shift_key: self.shift,
            ..SerializedPointInteraction::default()
        }
    }

    fn pointer_event(self) -> Rc<dyn Any> {
        let mut json = serde_json::to_value(self.point()).expect("point serializes");
        json.as_object_mut().expect("an object").extend(
            serde_json::json!({
                "pointer_id": self.id, "width": 1.0, "height": 1.0, "pressure": 0.5,
                "tangential_pressure": 0.0, "tilt_x": 0, "tilt_y": 0, "twist": 0,
                "pointer_type": self.kind, "is_primary": true,
            })
            .as_object()
            .expect("an object")
            .clone(),
        );
        let data: SerializedPointerData = serde_json::from_value(json).expect("pointer data");
        EventData::Pointer(data).into_any()
    }

    fn click_event(self) -> Rc<dyn Any> {
        let data: SerializedMouseData =
            serde_json::from_value(serde_json::to_value(self.point()).expect("point serializes"))
                .expect("mouse data");
        EventData::Mouse(data).into_any()
    }
}

struct Rig {
    dom: VirtualDom,
    view: Rc<View<()>>,
    recorder: Recorder,
}

impl Rig {
    fn new(input: DiffInput, virtualized: bool, expand_unchanged: bool, interaction: bool) -> Self {
        // Event payloads are the serialized forms; without a converter `event.data()` panics.
        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        let recorder = Recorder::default();
        let mut dom = VirtualDom::new_with_props(
            Page,
            PageProps {
                recorder: recorder.clone(),
                input,
                virtualized,
                expand_unchanged,
                interaction,
            },
        );
        dom.rebuild_in_place();
        let view = recorder.view.borrow().clone().expect("the page rendered");
        Self {
            dom,
            view,
            recorder,
        }
    }

    fn document(&self) -> &Document {
        super::host_document(&self.view.host)
    }

    fn shadow(&self) -> NodeKey {
        self.document()
            .shadow_root(self.view.host.container())
            .expect("a rendered diff")
    }

    fn query(&self, attribute: &str, value: Option<&str>) -> Vec<NodeKey> {
        self.document()
            .query_by_attribute(self.shadow(), attribute, value)
    }

    /// The `pre` Pierre renders the rows into, the node the manager listens on.
    fn pre(&self) -> NodeKey {
        self.query("data-diff-type", None)
            .into_iter()
            .find(|node| self.document().tag_name(node) == "PRE")
            .expect("the diff has a pre")
    }

    /// The content row of the context line `number` (it is unique, unlike a changed line's).
    fn line(&self, number: usize) -> NodeKey {
        self.query("data-line", Some(&number.to_string()))[0]
    }

    /// Its line number in the gutter: the element a click starts a selection from.
    fn number(&self, number: usize) -> NodeKey {
        self.query("data-column-number", Some(&number.to_string()))[0]
    }

    /// Lets spawned tasks run and Dioxus apply the document's edits.
    fn settle(&mut self) {
        for _ in 0..3 {
            let _polled: Option<()> = self.dom.wait_for_work().now_or_never();
        }
        self.dom.render_immediate_to_vec();
    }

    /// Delivers one event the way a browser bubbles it: the handlers of the node it hit, then of
    /// each ancestor up to the top node, all given the same event; a handler that stops
    /// propagation ends it. Returns whether the manager prevented the default action.
    fn bubble(&mut self, target: NodeKey, name: &str, data: Rc<dyn Any>) -> bool {
        let document = self.document();
        let mut chain = vec![target];
        while let Some(parent) = document.parent(*chain.last().expect("a chain")) {
            chain.push(parent);
        }
        let pointers = Rc::clone(&self.view.pointers);
        let event = Event::new(data, true);
        let handler = format!("on{name}");
        self.dom.in_scope(ScopeId::ROOT, || {
            // The browser decides who hears the event before any of them runs (a handler may
            // replace the nodes above it).
            let heard: Vec<Vec<Attribute>> = chain
                .into_iter()
                .map(|node| pointers.listeners(node))
                .collect();
            for attributes in heard {
                for attribute in attributes {
                    if attribute.name == handler
                        && let AttributeValue::Listener(callback) = &attribute.value
                    {
                        callback.call(event.clone());
                    }
                }
                if !event.propagates() {
                    break;
                }
            }
        });
        self.settle();
        !event.default_action_enabled()
    }

    fn pointer(&mut self, target: NodeKey, name: &str, pointer: Pointer) -> bool {
        self.bubble(target, name, pointer.pointer_event())
    }

    fn click(&mut self, target: NodeKey, pointer: Pointer) -> bool {
        self.bubble(target, "click", pointer.click_event())
    }

    fn selected_lines(&self) -> Vec<String> {
        self.query("data-selected-line", None)
            .into_iter()
            .filter_map(|node| self.document().get_attribute(node, "data-line"))
            .collect()
    }

    fn last_selection(&self) -> Option<SelectedLineRange> {
        self.recorder
            .selected
            .borrow()
            .last()
            .expect("the callback was called")
            .clone()
    }
}

fn range(start: usize, end: usize) -> SelectedLineRange {
    SelectedLineRange {
        start,
        side: Some(pierre_diffs::types::view::SelectionSide::Additions),
        end,
        end_side: None,
    }
}

#[test]
fn rows_get_pointer_handlers_only_when_the_options_give_the_pointer_something_to_do() {
    let listeners = |interaction| {
        let recorder = Recorder::default();
        let mut dom = VirtualDom::new_with_props(
            Page,
            PageProps {
                recorder,
                input: pair(5, &[3]),
                virtualized: false,
                expand_unchanged: true,
                interaction,
            },
        );
        let mut names: Vec<String> = dom
            .rebuild_to_vec()
            .edits
            .iter()
            .filter_map(|mutation| match mutation {
                Mutation::NewEventListener { name, .. } => Some(name.to_string()),
                _ => None,
            })
            .collect();
        names.sort();
        names.dedup();
        names
    };
    // Upstream's manager listens whenever hunks expand on click; a view that only expands
    // hears the clicks (the `pre`'s and the separators'), not every move and press.
    assert_eq!(listeners(false), ["click"]);
    assert_eq!(
        listeners(true),
        [
            "click",
            // The rows' heights and the `pre`'s and a column's client rects, which a drag's
            // hit-testing reads.
            "mounted",
            "pointercancel",
            "pointerdown",
            "pointerleave",
            "pointermove",
            "pointerup",
            "resize"
        ]
    );
}

#[test]
fn hovering_a_line_sets_data_hovered_and_leaving_the_view_clears_it() {
    let mut rig = Rig::new(pair(5, &[3]), false, true, true);
    let (line, number) = (rig.line(2), rig.number(2));
    rig.pointer(line, "pointermove", Pointer::mouse(10.0, 10.0));
    assert!(rig.document().has_attribute(line, "data-hovered"));
    assert!(rig.document().has_attribute(number, "data-hovered"));
    assert!(dioxus_ssr::render(&rig.dom).contains("data-hovered"));
    // Another line moves it; no other line holds it.
    let next = rig.line(4);
    rig.pointer(next, "pointermove", Pointer::mouse(10.0, 50.0));
    assert!(!rig.document().has_attribute(line, "data-hovered"));
    assert!(rig.document().has_attribute(next, "data-hovered"));
    // `pointerleave` does not bubble: only the `pre`'s own handler hears it.
    let pre = rig.pre();
    rig.pointer(pre, "pointerleave", Pointer::mouse(900.0, 10.0));
    assert!(!rig.document().has_attribute(next, "data-hovered"));
    assert_eq!(rig.query("data-hovered", None), []);
}

#[test]
fn a_touch_move_is_not_a_hover() {
    let mut rig = Rig::new(pair(5, &[3]), false, true, true);
    let line = rig.line(2);
    let touch = Pointer {
        kind: "touch",
        ..Pointer::mouse(10.0, 10.0)
    };
    rig.pointer(line, "pointermove", touch);
    assert_eq!(rig.query("data-hovered", None), []);
}

#[test]
fn clicking_a_line_number_selects_the_line_and_shift_click_extends_the_range() {
    let mut rig = Rig::new(pair(5, &[3]), false, true, true);
    let (two, four) = (rig.number(2), rig.number(4));
    let press = Pointer::mouse(5.0, 30.0).pressed();
    rig.pointer(two, "pointerdown", press);
    rig.pointer(two, "pointerup", Pointer { button: 0, ..press }.dragging());
    assert_eq!(rig.selected_lines(), ["2"]);
    assert_eq!(rig.last_selection(), Some(range(2, 2)));
    assert!(dioxus_ssr::render(&rig.dom).contains("data-selected-line"));

    // Shift extends from the anchor to the line pressed.
    let press = Pointer::mouse(5.0, 90.0).pressed().shift();
    rig.pointer(four, "pointerdown", press);
    rig.pointer(four, "pointerup", press);
    assert_eq!(rig.last_selection(), Some(range(2, 4)));
    let lines = rig.selected_lines();
    assert!(
        ["2", "3", "4"]
            .iter()
            .all(|number| lines.contains(&number.to_string()))
    );
    assert!(!lines.contains(&"1".to_owned()) && !lines.contains(&"5".to_owned()));
}

#[test]
fn one_event_reaches_the_manager_once_however_many_handlers_it_bubbles_through() {
    let rig = Rig::new(pair(5, &[3]), false, true, true);
    let pointers = &rig.view.pointers;
    let event = Pointer::mouse(1.0, 1.0).pointer_event();
    let data = |event: &Rc<dyn Any>| {
        Rc::clone(event)
            .downcast::<dioxus::html::PlatformEventData>()
            .expect("the payload is platform data")
    };
    // The number's handler is first, the `pre`'s and the rest see the same event after it.
    assert!(pointers.first_at_node(&data(&event)));
    assert!(!pointers.first_at_node(&data(&event)));
    // The top node's handler knows the event was already delivered to the document listeners.
    assert!(!pointers.first_at_document(&data(&event)));
    // The next event is a different payload and is taken again.
    let next = Pointer::mouse(2.0, 2.0).pointer_event();
    assert!(pointers.first_at_document(&data(&next)));
    assert!(pointers.first_at_node(&data(&next)));
}

/// The scripted drag: the press at the first number, the pointer moving over the rows while the
/// event keeps naming the node the drag began on (what a captured touch does).
fn drag_rig() -> (Rig, [f64; 2]) {
    let mut rig = Rig::new(pair(8, &[7]), true, true, true);
    let host = Rc::clone(&rig.view.host);
    let scroller = host.scroller().expect("a scrolling host");
    let (root, content, container) = (scroller.root, scroller.content, host.container());
    let (pre, column) = (rig.pre(), rig.query("data-content", None)[0]);
    let height = match &rig.view.engine {
        Engine::Virtualized { instance, .. } => instance.get_virtualized_height(),
        Engine::Static(_) => unreachable!("a virtualized rig"),
    };
    let dispatch = Rc::clone(&rig.view.dispatch);
    // The pre sits at x 100, 800 wide; its first row starts at y 50.
    rig.dom.in_scope(ScopeId::ROOT, || {
        dispatch.mounted(root, MountedData::new(rect(100.0, 0.0, 800.0, 600.0)));
        dispatch.mounted(pre, MountedData::new(rect(100.0, 50.0, 800.0, 400.0)));
        dispatch.mounted(column, MountedData::new(rect(140.0, 50.0, 760.0, 400.0)));
        dispatch.resized(root, 600.0, 800.0);
        dispatch.resized(content, height, 800.0);
        dispatch.visible(container, true);
    });
    rig.settle();
    (rig, [50.0, 20.0])
}

#[test]
fn a_drag_selects_the_rows_under_the_pointer_from_the_cached_geometry() {
    let (mut rig, [first_row_top, row_height]) = drag_rig();
    let start = rig.number(1);
    let row_y = |index: usize| first_row_top + row_height * (index as f64 + 0.5);
    // Rows 1 to 4 are unchanged lines 1 to 4, row 5 and 6 the changed line 5's pair.
    rig.pointer(
        start,
        "pointerdown",
        Pointer::mouse(120.0, row_y(0)).pressed(),
    );
    assert!(rig.view.host.pointer.document_listeners_active());
    // `settle` ran the task that measures the frame, so the point now answers with its row.
    let hit = pierre_diffs::managers::interaction_manager::InteractionHost::element_from_point(
        &*rig.view.host,
        500.0,
        row_y(3),
    );
    assert_eq!(
        hit,
        pierre_diffs::managers::interaction_manager::HitTest::Element(rig.line(4))
    );
    // The events name the first number; the coordinates name the rows.
    for index in 1..=3 {
        rig.pointer(
            start,
            "pointermove",
            Pointer::mouse(500.0, row_y(index)).dragging(),
        );
    }
    let lines = rig.selected_lines();
    assert!(
        ["1", "2", "3", "4"]
            .iter()
            .all(|number| lines.contains(&number.to_string())),
        "{lines:?}"
    );
    // The release is not on any handler of a row: the top node hears it.
    let top = rig.view.host.top();
    rig.pointer(top, "pointerup", Pointer::mouse(500.0, row_y(3)));
    assert_eq!(rig.last_selection(), Some(range(1, 4)));
    assert!(!rig.view.host.pointer.document_listeners_active());
    // A finished drag keeps no frame, so nothing answers a coordinate until the next one.
    assert_eq!(
        pierre_diffs::managers::interaction_manager::InteractionHost::element_from_point(
            &*rig.view.host,
            500.0,
            row_y(3),
        ),
        pierre_diffs::managers::interaction_manager::HitTest::Unavailable,
    );
}

/// A non-virtualized view with the platform's part scripted: the `pre` at x 100, 800 wide, its
/// first row at y 50, every row 20 high, delivered as the `onmounted` and `onresize` the
/// renderer asked for.
fn static_drag_rig() -> Rig {
    let mut rig = Rig::new(pair(8, &[7]), false, true, true);
    let (pre, column) = (rig.pre(), rig.query("data-content", None)[0]);
    let rows = rig.document().child_nodes(column);
    let dispatch = Rc::clone(&rig.view.dispatch);
    rig.dom.in_scope(ScopeId::ROOT, || {
        let hears = |node, name: &str| {
            dispatch
                .listeners(node)
                .iter()
                .any(|attribute| attribute.name == name)
        };
        // Only what the renderer attached a listener to hears anything.
        assert!(hears(pre, "onmounted") && hears(column, "onmounted"));
        assert!(!hears(column, "onresize"), "a column is not a row");
        dispatch.mounted(pre, MountedData::new(rect(100.0, 50.0, 800.0, 400.0)));
        dispatch.mounted(column, MountedData::new(rect(140.0, 50.0, 760.0, 400.0)));
        for row in rows
            .iter()
            .filter(|row| !rig.document().tag_name(*row).is_empty())
        {
            assert!(hears(*row, "onresize"), "a row is measured");
            dispatch.resized(*row, 20.0, 760.0);
        }
    });
    rig.settle();
    rig
}

#[test]
fn a_touch_drag_across_the_rows_of_a_non_virtualized_view_selects_the_range() {
    let mut rig = static_drag_rig();
    let start = rig.number(1);
    let row_y = |index: usize| 50.0 + 20.0 * (index as f64 + 0.5);
    let touch = |x, y| Pointer {
        kind: "touch",
        ..Pointer::mouse(x, y)
    };
    rig.pointer(
        start,
        "pointerdown",
        Pointer {
            button: 0,
            buttons: 1,
            ..touch(120.0, row_y(0))
        },
    );
    assert!(rig.view.host.pointer.document_listeners_active());
    // The frame was measured with no scroller to read.
    assert_eq!(
        pierre_diffs::managers::interaction_manager::InteractionHost::element_from_point(
            &*rig.view.host,
            500.0,
            row_y(3),
        ),
        pierre_diffs::managers::interaction_manager::HitTest::Element(rig.line(4))
    );
    // A touch keeps naming the row the drag began on; the coordinates name the rows.
    for index in 1..=3 {
        rig.pointer(
            start,
            "pointermove",
            Pointer {
                buttons: 1,
                ..touch(500.0, row_y(index))
            },
        );
    }
    let top = rig.view.host.top();
    rig.pointer(top, "pointerup", touch(500.0, row_y(3)));
    assert_eq!(rig.last_selection(), Some(range(1, 4)));
    assert!(!rig.view.host.pointer.document_listeners_active());
}

#[test]
fn a_view_whose_pointer_has_no_rows_to_act_on_measures_none() {
    let rig = Rig::new(pair(5, &[3]), false, true, false);
    let (line, pre) = (rig.line(2), rig.pre());
    let dispatch = Rc::clone(&rig.view.dispatch);
    rig.dom.in_scope(ScopeId::ROOT, || {
        for node in [line, pre] {
            assert!(dispatch.listeners(node).is_empty());
        }
    });
}

/// A collapsed hunk's separator sits between rows of the content column and is as tall as the
/// stylesheet makes it, which no `style` in the document says. Every node that hears `onresize`
/// reports its scripted height (a line 20, a separator 32), as the platform would.
#[test]
fn a_touch_drag_across_a_collapsed_hunk_separator_lands_on_the_row_below_it() {
    for virtualized in [false, true] {
        let mut rig = Rig::new(pair(30, &[3, 20]), virtualized, false, true);
        let host = Rc::clone(&rig.view.host);
        let dispatch = Rc::clone(&rig.view.dispatch);
        // The virtualized view renders its window once the scroller reports its size; the nodes
        // below are those of the settled render.
        if let Some(scroller) = host.scroller() {
            let (root, content, container) = (scroller.root, scroller.content, host.container());
            let content_height = match &rig.view.engine {
                Engine::Virtualized { instance, .. } => instance.get_virtualized_height(),
                Engine::Static(_) => unreachable!("a virtualized view"),
            };
            rig.dom.in_scope(ScopeId::ROOT, || {
                dispatch.mounted(root, MountedData::new(rect(100.0, 0.0, 800.0, 600.0)));
                dispatch.resized(root, 600.0, 800.0);
                dispatch.resized(content, content_height, 800.0);
                dispatch.visible(container, true);
            });
            rig.settle();
        }
        let (pre, column) = (rig.pre(), rig.query("data-content", None)[0]);
        let height_of = |document: &Document, node: NodeKey| {
            if document.has_attribute(node, "data-separator") {
                32.0
            } else {
                20.0
            }
        };
        let rows: Vec<NodeKey> = rig
            .document()
            .child_nodes(column)
            .into_iter()
            .filter(|row| !rig.document().tag_name(*row).is_empty())
            .collect();
        let at_separator = rows
            .iter()
            .position(|row| rig.document().has_attribute(*row, "data-separator"))
            .expect("a collapsed hunk renders a separator in the column");
        let (above, separator, below) = (
            rows[at_separator - 1],
            rows[at_separator],
            rows[at_separator + 1],
        );
        rig.dom.in_scope(ScopeId::ROOT, || {
            let hears = |node| {
                dispatch
                    .listeners(node)
                    .iter()
                    .any(|attribute| attribute.name == "onresize")
            };
            dispatch.mounted(pre, MountedData::new(rect(100.0, 50.0, 800.0, 600.0)));
            dispatch.mounted(column, MountedData::new(rect(140.0, 50.0, 760.0, 600.0)));
            for row in &rows {
                if hears(*row) {
                    dispatch.resized(*row, height_of(rig.document(), *row), 760.0);
                }
            }
        });
        rig.settle();
        let start = rig.number(1);
        let touch = |x, y| Pointer {
            kind: "touch",
            buttons: 1,
            ..Pointer::mouse(x, y)
        };
        rig.pointer(
            start,
            "pointerdown",
            Pointer {
                button: 0,
                ..touch(120.0, 60.0)
            },
        );
        // The client top of `below`: the first row at y 50, then each row's scripted height.
        let top_of = |target: NodeKey| {
            50.0 + rows
                .iter()
                .take_while(|row| **row != target)
                .map(|row| height_of(rig.document(), *row))
                .sum::<f64>()
        };
        let at = |node: NodeKey, offset: f64| {
            pierre_diffs::managers::interaction_manager::InteractionHost::element_from_point(
                &*rig.view.host,
                500.0,
                top_of(node) + offset,
            )
        };
        use pierre_diffs::managers::interaction_manager::HitTest::Element;
        assert_eq!(
            at(above, 10.0),
            Element(above),
            "virtualized: {virtualized}"
        );
        assert_eq!(
            at(separator, 16.0),
            Element(separator),
            "virtualized: {virtualized}"
        );
        assert_eq!(
            at(below, 10.0),
            Element(below),
            "virtualized: {virtualized}"
        );
    }
}

#[test]
fn a_drag_whose_button_came_up_outside_the_view_ends_on_the_next_move() {
    let mut rig = Rig::new(pair(5, &[3]), false, true, true);
    let (two, four) = (rig.number(2), rig.number(4));
    rig.pointer(two, "pointerdown", Pointer::mouse(5.0, 30.0).pressed());
    assert!(rig.view.host.pointer.document_listeners_active());
    // The button was released outside the window; the next move in has no button held.
    rig.pointer(four, "pointermove", Pointer::mouse(5.0, 90.0));
    assert!(!rig.view.host.pointer.document_listeners_active());
    assert_eq!(rig.last_selection(), Some(range(2, 2)));
    // So a new press is taken (a stuck session would ignore it).
    rig.pointer(four, "pointerdown", Pointer::mouse(5.0, 90.0).pressed());
    rig.pointer(four, "pointerup", Pointer::mouse(5.0, 90.0));
    assert_eq!(rig.last_selection(), Some(range(4, 4)));
}

#[test]
fn clicking_a_collapsed_hunks_expand_button_shows_the_hidden_lines() {
    // Two changes far apart: the 30 unchanged lines between them collapse behind a separator.
    let mut rig = Rig::new(pair(40, &[2, 39]), false, false, true);
    let rows = |rig: &Rig| rig.query("data-line", None).len();
    let before = rows(&rig);
    assert!(before < 40, "{before} rows, the middle should be collapsed");
    let button = rig.query("data-expand-button", None);
    let button = *button
        .first()
        .expect("a collapsed hunk has an expand button");
    rig.click(button, Pointer::mouse(20.0, 200.0).pressed());
    assert!(
        rows(&rig) > before,
        "expanding shows more rows than {before}"
    );
}

#[test]
fn dropping_the_view_after_a_render_replaced_the_hovered_rows_cleans_up() {
    let mut rig = Rig::new(pair(40, &[2, 39]), false, false, true);
    let hovered = rig.line(1);
    rig.pointer(hovered, "pointermove", Pointer::mouse(10.0, 10.0));
    assert!(rig.document().has_attribute(hovered, "data-hovered"));
    let button = rig.query("data-expand-button", None)[0];
    rig.click(button, Pointer::mouse(20.0, 200.0).pressed());
    // `use_drop` runs `FileDiff::clean_up`, which clears the hover it still holds.
    drop(rig);
}
