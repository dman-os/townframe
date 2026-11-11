//! The virtualized view in a headless `VirtualDom`, driven by synthesized events.
//!
//! No engine is here, so nothing lays anything out: the tests play the engine's part. They
//! deliver what a browser would (`onmounted` with a scripted element, `onresize`, `onvisible`,
//! `onscroll`) by calling the view's `Dispatch`, which is exactly what the attached listeners
//! call after converting the platform payload, and read back the document the managers edited.
//! The listeners' wiring (names, which nodes) is checked separately from the mutations Dioxus
//! emits.

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use dioxus::core::Mutation;
use dioxus::html::MountedData;
use dioxus::html::MountedResult;
use dioxus::html::RenderedElementBacking;
use dioxus::html::ScrollBehavior;
use dioxus::html::geometry::PixelsVector2D;
use futures::FutureExt;
use futures::channel::oneshot;
use pierre_diffs::components::virtualizer::InstanceRef;
use pierre_diffs::components::virtualizer::Root;
use pierre_diffs::components::virtualizer::SubscribedInstance;
use pierre_diffs::components::virtualizer::Virtualizer;
use pierre_diffs::components::virtualizer::VirtualizerConfig;
use pierre_diffs::components::virtualizer::VirtualizerHost;
use pierre_diffs::document::NodeKey;
use pierre_diffs::js_collections::PtrEq;
use pierre_diffs::managers::scroll_sync_manager::ScrollSyncHost;
use pierre_diffs::managers::universal_rendering_manager::AnimationFrames;
use pierre_diffs::managers::universal_rendering_manager::UniversalRenderingManager;
use pierre_diffs::types::patch::FileContents;
use pierre_diffs::utils::create_window_from_scroll_position::WindowFromScrollPositionProps;
use pierre_diffs::utils::create_window_from_scroll_position::create_window_from_scroll_position;

use super::*;
use crate::geometry::ScrollSample;
use crate::host::DioxusHost;

const VIEWPORT: f64 = 600.0;

/// An element whose scroll commands are recorded, standing in for the engine's.
#[derive(Default)]
struct Scripted {
    offset: Cell<(f64, f64)>,
    log: RefCell<Vec<String>>,
}

struct ScriptedElement(Rc<Scripted>);

impl RenderedElementBacking for ScriptedElement {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_scroll_offset(&self) -> Pin<Box<dyn Future<Output = MountedResult<PixelsVector2D>>>> {
        let scripted = Rc::clone(&self.0);
        Box::pin(async move {
            scripted.log.borrow_mut().push("read".to_owned());
            let (x, y) = scripted.offset.get();
            Ok(PixelsVector2D::new(x, y))
        })
    }

    fn scroll(
        &self,
        coordinates: PixelsVector2D,
        behavior: ScrollBehavior,
    ) -> Pin<Box<dyn Future<Output = MountedResult<()>>>> {
        let scripted = Rc::clone(&self.0);
        Box::pin(async move {
            scripted.log.borrow_mut().push(format!(
                "scroll {} {} {behavior:?}",
                coordinates.x, coordinates.y
            ));
            scripted.offset.set((coordinates.x, coordinates.y));
            Ok(())
        })
    }
}

/// A clock the test winds by hand: each `sleep` is a channel the test fires.
#[derive(Default, Clone)]
struct Clock(Rc<RefCell<Vec<Sleeper>>>);

type Sleeper = (Duration, oneshot::Sender<()>);

impl Clock {
    fn sleep(&self) -> Rc<dyn Fn(Duration) -> SleepFuture> {
        let clock = self.clone();
        Rc::new(move |duration| {
            let (sender, receiver) = oneshot::channel();
            clock.0.borrow_mut().push((duration, sender));
            Box::pin(async move {
                // A dropped sender (the task was cancelled) never resolves.
                if receiver.await.is_err() {
                    std::future::pending::<()>().await;
                }
            })
        })
    }

    fn fire(&self, index: usize) -> Duration {
        let (duration, sender) = self.0.borrow_mut().remove(index);
        sender.send(()).expect("the sleeping task is alive");
        duration
    }
}

#[derive(Clone, Default)]
struct Shared(Rc<RefCell<Option<Rc<View<()>>>>>);

impl PartialEq for Shared {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

#[derive(Props, Clone, PartialEq)]
struct PageProps {
    shared: Shared,
    lines: usize,
    clock: PtrEq<dyn Fn(Duration) -> SleepFuture>,
}

fn file(contents: String) -> Box<FileContents> {
    Box::new(FileContents {
        name: "big.rs".to_owned(),
        contents,
        lang: None,
        header: None,
        cache_key: None,
    })
}

/// `lines` lines, with every 97th changed.
fn big_pair(lines: usize) -> DiffInput {
    let line = |index: usize, changed: bool| {
        if changed {
            format!("let value_{index} = changed({index});")
        } else {
            format!("let value_{index} = {index};")
        }
    };
    let old = (0..lines)
        .map(|index| line(index, false))
        .collect::<Vec<_>>();
    let new = (0..lines)
        .map(|index| line(index, index % 97 == 0 && index > 0))
        .collect::<Vec<_>>();
    DiffInput::Files {
        old_file: file(old.join("\n")),
        new_file: file(new.join("\n")),
    }
}

#[component]
fn Page(props: PageProps) -> Element {
    let lines = props.lines;
    let input = use_hook(|| big_pair(lines));
    let view = use_view(&FileDiffViewProps::<()> {
        input,
        diff_style: DiffStyle::Unified,
        expand_unchanged: true,
        annotations: Vec::new(),
        render_annotation: None,
        highlighter: None,
        virtualized: Some(Virtualization {
            height: format!("{VIEWPORT}px"),
            sleep: props.clock.clone(),
        }),
    });
    *props.shared.0.borrow_mut() = Some(Rc::clone(&view));
    view.element()
}

struct Rig {
    dom: VirtualDom,
    view: Rc<View<()>>,
    root: Rc<Scripted>,
}

impl Rig {
    fn new(lines: usize) -> Self {
        let shared = Shared::default();
        let mut dom = VirtualDom::new_with_props(
            Page,
            PageProps {
                shared: shared.clone(),
                lines,
                clock: PtrEq::new(Clock::default().sleep()),
            },
        );
        dom.rebuild_in_place();
        let view = shared.0.borrow().clone().expect("the page rendered");
        Self {
            dom,
            view,
            root: Rc::default(),
        }
    }

    fn dispatch(&self) -> &Rc<Dispatch> {
        self.view.dispatch.as_ref().expect("a virtualized view")
    }

    fn nodes(&self) -> (NodeKey, NodeKey, NodeKey) {
        let scroller = self.view.host.scroller().expect("a scrolling host");
        (scroller.root, scroller.content, self.view.host.container())
    }

    /// Runs `events` as one dispatch would (in the runtime, so handlers can spawn), then lets
    /// spawned scroll tasks run and Dioxus apply the document's edits.
    fn deliver(&mut self, events: impl FnOnce(&Rig)) {
        self.dom.in_scope(ScopeId::ROOT, || events(self));
        self.settle();
    }

    fn settle(&mut self) {
        run_tasks(&mut self.dom);
        self.dom.render_immediate_to_vec();
    }

    /// What a browser reports once the page is up: the scroller mounted, the scroller and its
    /// content sized, the diff in view.
    fn boot(&mut self) {
        let (root, content, container) = self.nodes();
        let height = self.estimated_height();
        let scripted = Rc::clone(&self.root);
        self.deliver(|rig| {
            let dispatch = rig.dispatch();
            dispatch.mounted(root, MountedData::new(ScriptedElement(scripted)));
            dispatch.resized(root, VIEWPORT, 800.0);
            dispatch.resized(content, height, 800.0);
            dispatch.visible(container, true);
        });
    }

    fn estimated_height(&self) -> f64 {
        match &self.view.engine {
            Engine::Virtualized { instance, .. } => instance.get_virtualized_height(),
            Engine::Static(_) => unreachable!("a virtualized rig"),
        }
    }

    fn scroll_to(&mut self, top: f64) {
        let (root, ..) = self.nodes();
        let height = self.estimated_height();
        self.root.offset.set((0.0, top));
        self.deliver(|rig| {
            rig.dispatch().scrolled(
                root,
                ScrollSample {
                    top,
                    left: 0.0,
                    scroll_height: height,
                    client_height: VIEWPORT,
                },
            );
        });
    }

    /// The line numbers the document holds rows for, in order.
    fn rendered_lines(&self) -> Vec<usize> {
        let document = super::host_document(&self.view.host);
        let container = self.view.host.container();
        let shadow = document.shadow_root(container).expect("a rendered diff");
        document
            .query_by_attribute(shadow, "data-line-index", None)
            .into_iter()
            .filter_map(|row| document.get_attribute(row, "data-line"))
            .map(|number| number.parse().expect("a line number"))
            .collect()
    }
}

/// Lets spawned tasks run: one poll of the dom's work future runs every ready task.
fn run_tasks(dom: &mut VirtualDom) {
    for _ in 0..3 {
        let _polled: Option<()> = dom.wait_for_work().now_or_never();
    }
}

#[component]
fn Empty() -> Element {
    rsx! {}
}

#[test]
fn only_the_window_around_the_viewport_is_rendered_and_it_follows_scrolling() {
    let mut rig = Rig::new(3000);
    // `setup` and `connect` queued the virtualizer's pass; no event has ended a dispatch yet.
    assert!(rig.view.host.frames_pending() > 0);
    rig.boot();
    let top = rig.rendered_lines();
    // The first window: the viewport plus the overscan below, nothing from the far end.
    assert_eq!(top.first(), Some(&1));
    assert!(
        top.len() < 120 && top.last().is_some_and(|last| *last < 120),
        "rendered {} lines, last {:?}",
        top.len(),
        top.last()
    );
    // And Dioxus renders exactly the document's rows (the first and a late one, not a far one).
    let page = dioxus_ssr::render(&rig.dom);
    assert!(page.contains("value_5 = 5"));
    assert!(!page.contains("value_1500 = 1500"));

    rig.scroll_to(30_000.0);
    let middle = rig.rendered_lines();
    // The viewport (rows 1500 to 1530 at 20 px) is rendered, with the overscan around it and
    // nothing like the whole file.
    let (first, last) = (middle[0], middle[middle.len() - 1]);
    assert!(
        (1200..=1450).contains(&first) && (1530..=1800).contains(&last) && middle.len() < 600,
        "rows {first}..={last} ({} rows) after scrolling to 30000",
        middle.len()
    );
    let page = dioxus_ssr::render(&rig.dom);
    assert!(page.contains("value_1500 = 1500"));
    assert!(!page.contains("value_5 = 5"));

    // The oracle-tested window function, fed what the host was told, is the window the
    // virtualizer settled on.
    let Engine::Virtualized { virtualizer, .. } = &rig.view.engine else {
        unreachable!("a virtualized rig")
    };
    let expected = create_window_from_scroll_position(WindowFromScrollPositionProps::new(
        30_000.0,
        VIEWPORT,
        rig.estimated_height(),
        pierre_diffs::components::virtualizer::DEFAULT_OVERSCROLL_SIZE,
    ));
    assert_eq!(virtualizer.get_window_specs(), expected);
}

#[test]
fn the_nodes_the_port_registered_are_the_ones_rendered_with_listeners() {
    let shared = Shared::default();
    let mut dom = VirtualDom::new_with_props(
        Page,
        PageProps {
            shared: shared.clone(),
            lines: 400,
            clock: PtrEq::new(Clock::default().sleep()),
        },
    );
    let mutations = dom.rebuild_to_vec();
    let mut listeners: Vec<String> = mutations
        .edits
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::NewEventListener { name, .. } => Some(name.to_string()),
            _ => None,
        })
        .collect();
    listeners.sort();
    // The scroller listens for scroll, mounting and size; its content and the container for
    // size, the container also for visibility; rows and the header for size.
    for expected in ["mounted", "resize", "scroll", "visible"] {
        assert!(
            listeners.iter().any(|name| name == expected),
            "no {expected} listener in {listeners:?}"
        );
    }
    assert_eq!(listeners.iter().filter(|name| *name == "scroll").count(), 1);
    assert_eq!(
        listeners.iter().filter(|name| *name == "mounted").count(),
        1
    );
    assert_eq!(
        listeners.iter().filter(|name| *name == "visible").count(),
        1
    );
}

#[test]
fn a_dispatch_runs_the_frames_it_queued_and_a_follow_up_waits_for_the_next() {
    let host = DioxusHost::new_scrolling("10px", Clock::default().sleep(), 20.0);
    let ran = Rc::new(Cell::new(0));
    let queue = {
        let (ran, host) = (Rc::clone(&ran), Rc::clone(&host));
        move || {
            ran.set(ran.get() + 1);
            // The render queue's follow-up frame.
            let ran = Rc::clone(&ran);
            host.request_animation_frame(Box::new(move |_| ran.set(ran.get() + 10)));
        }
    };
    host.request_animation_frame(Box::new(move |_| queue()));
    let cancelled = host.request_animation_frame(Box::new(|_| unreachable!("cancelled")));
    host.cancel_animation_frame(cancelled);
    assert_eq!(ran.get(), 0, "nothing runs before the dispatch ends");
    assert_eq!(host.frames_pending(), 1);
    host.flush_frames();
    assert_eq!(ran.get(), 1, "the queued frame ran, its follow-up did not");
    assert_eq!(host.frames_pending(), 1);
    host.flush_frames();
    assert_eq!(ran.get(), 11);
    assert_eq!(host.frames_pending(), 0);
}

/// A file made of rows the test lays out itself, so the scroll fix has an edit to see.
struct Stack {
    buffer: NodeKey,
    rows: Vec<NodeKey>,
}

/// An instance whose render grows the buffer above the rows: what a window moving down does
/// when the rows it drops were shorter than the estimate it replaces them with.
struct Grower {
    host: Rc<DioxusHost>,
    buffer: NodeKey,
    grow_to: Cell<Option<f64>>,
}

impl SubscribedInstance for Grower {
    fn on_render(&self, _dirty: bool) -> bool {
        if let Some(height) = self.grow_to.take() {
            super::host_document(&self.host).set_style_property(
                self.buffer,
                "height",
                &format!("{height}px"),
            );
        }
        true
    }

    fn reconcile_heights(&self) -> bool {
        false
    }

    fn set_visibility(&self, _visible: bool) {}
}

fn as_instance(grower: &Rc<Grower>) -> InstanceRef {
    let concrete: Rc<Grower> = Rc::clone(grower);
    concrete
}

fn stack(host: &DioxusHost) -> Stack {
    let document = super::host_document(host);
    let shadow = document.attach_shadow(host.container());
    let buffer = document.create_element("div");
    document.set_style_property(buffer, "height", "100px");
    let pre = document.create_element("pre");
    let content = document.create_element("div");
    document.set_attribute(content, "data-content", "");
    document.append_child(shadow, buffer);
    document.append_child(shadow, pre);
    document.append_child(pre, content);
    let rows = (0..100)
        .map(|index| {
            let row = document.create_element("div");
            document.set_attribute(row, "data-line", &(index + 1).to_string());
            document.set_attribute(row, "data-line-index", &index.to_string());
            document.append_child(content, row);
            row
        })
        .collect();
    Stack { buffer, rows }
}

#[test]
fn a_height_change_above_the_viewport_reads_the_offset_then_scrolls_by_the_delta() {
    let host = DioxusHost::new_scrolling("600px", Clock::default().sleep(), 20.0);
    let stack = stack(&host);
    let (root, content) = {
        let scroller = host.scroller().expect("a scrolling host");
        (scroller.root, scroller.content)
    };
    let queue = UniversalRenderingManager::new(Rc::clone(&host));
    let virtualizer = Virtualizer::new(Rc::clone(&host), queue, VirtualizerConfig::default());
    virtualizer.setup(Root::Element(root), Some(content));
    let grower = Rc::new(Grower {
        host: Rc::clone(&host),
        buffer: stack.buffer,
        grow_to: Cell::new(None),
    });
    let instance = as_instance(&grower);
    let container = host.container();
    let _disconnect = virtualizer.connect(container, instance);

    // A page the engine has laid out: the buffer 100, rows 20 each, scrolled to 400 (row 15 is
    // the first one fully in view).
    let scripted = Rc::new(Scripted::default());
    scripted.offset.set((0.0, 400.0));
    let mut dom = VirtualDom::new(Empty);
    dom.rebuild_in_place();
    dom.in_scope(ScopeId::ROOT, || {
        host.record_mounted(
            root,
            Rc::new(MountedData::new(ScriptedElement(Rc::clone(&scripted)))),
        );
        host.record_size(root, VIEWPORT);
        host.record_size(content, 5000.0);
        host.record_size(container, 5000.0);
        for row in &stack.rows {
            host.record_size(*row, 20.0);
        }
        host.record_scroll(
            root,
            ScrollSample {
                top: 400.0,
                left: 0.0,
                scroll_height: 5000.0,
                client_height: VIEWPORT,
            },
        );
    });
    let settle_pass = |dom: &mut VirtualDom| {
        dom.in_scope(ScopeId::ROOT, || {
            virtualizer.handle_intersection_change(&[
                pierre_diffs::components::virtualizer::IntersectionEntry {
                    target: container,
                    is_intersecting: true,
                },
            ]);
            host.flush_frames();
            // A scroll event: the pass it queues reads the scroller's height, which the anchor
            // search of the passes after it needs.
            virtualizer.handle_element_scroll();
            host.flush_frames();
        });
    };
    // The first passes see no change: the anchor is where it was.
    settle_pass(&mut dom);
    run_tasks(&mut dom);
    assert!(
        scripted.log.borrow().is_empty(),
        "{:?}",
        scripted.log.borrow()
    );

    // The next pass's render grows the buffer by 50: every row below moves down 50, the
    // anchor row among them, and the pass has to scroll to keep it where it was.
    grower.grow_to.set(Some(150.0));
    virtualizer.instance_changed(&as_instance(&grower), true);
    dom.in_scope(ScopeId::ROOT, || host.flush_frames());
    // Nothing has been written yet that waits on the read: the cache already moved by the delta.
    assert_eq!(host.element_scroll_top(&root), 450.0);
    run_tasks(&mut dom);
    assert_eq!(
        *scripted.log.borrow(),
        ["read", "scroll 0 450 Instant"],
        "one task reads the live offset, then scrolls by the delta"
    );
}

#[test]
fn a_timeout_is_a_task_on_the_embedders_clock_and_clearing_it_cancels_the_task() {
    let clock = Clock::default();
    let host = DioxusHost::new_scrolling("600px", clock.sleep(), 20.0);
    let mut dom = VirtualDom::new(Empty);
    dom.rebuild_in_place();
    let fired = Rc::new(Cell::new(0));
    let ids = dom.in_scope(ScopeId::ROOT, || {
        let (a, b) = (Rc::clone(&fired), Rc::clone(&fired));
        (
            host.set_timeout(Box::new(move || a.set(a.get() + 1)), 25),
            host.set_timeout(Box::new(move || b.set(b.get() + 10)), 50),
        )
    });
    run_tasks(&mut dom);
    assert_eq!(clock.0.borrow().len(), 2);
    dom.in_scope(ScopeId::ROOT, || {
        host.clear_timeout(ids.1);
        host.clear_timeout(-1);
    });
    assert_eq!(clock.fire(0), Duration::from_millis(25));
    // The cancelled task's sleep is dropped with it; firing is a dead letter.
    run_tasks(&mut dom);
    assert_eq!(fired.get(), 1, "only the live timeout ran");
}
