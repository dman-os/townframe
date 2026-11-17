//! The code view in a headless `VirtualDom`, driven by synthesized events, and its host's trait
//! methods on their own.
//!
//! As in `view/virtual_tests.rs`, no engine is here: the tests play the engine's part by
//! calling the view's `Dispatch` (what the attached listeners call after converting the
//! platform payload), with `MountedData` over a scripted element, and read back the document
//! the viewer edited and the mutations Dioxus emitted.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use dioxus::core::AttributeValue;
use dioxus::core::Mutation;
use dioxus::html::MountedData;
use pierre_diffs::components::code_view::CodeViewHost;
use pierre_diffs::document::NodeKey;
use pierre_diffs::managers::universal_rendering_manager::AnimationFrames;
use pierre_diffs::types::view::AnnotationSide;
use pierre_diffs::types::view::CodeViewScrollBehavior;
use pierre_diffs::types::view::ScrollAlign;
use pierre_diffs::utils::dom::platform::PlatformInfo;

use super::*;
use crate::geometry::ScrollSample;
use crate::host::SlabRole;
use crate::view::virtual_tests::Clock;
use crate::view::virtual_tests::Scripted;
use crate::view::virtual_tests::ScriptedElement;
use crate::view::virtual_tests::run_tasks;

const VIEWPORT: f64 = 600.0;
const LINES: usize = 400;
/// The tail of the slab's `style` while a scroll burst has pointer interaction suspended (the
/// core stylesheet in the page has a `pointer-events:none` of its own, so the property alone
/// proves nothing).
const SUSPENDED_SLAB: &str = "gap:8px;pointer-events:none;";

type Note = &'static str;

fn file(name: &str, contents: String) -> FileContents {
    FileContents {
        name: name.to_owned(),
        contents,
        lang: None,
        header: None,
        cache_key: None,
    }
}

/// `LINES` lines of `name`, with every 40th changed.
fn changed_file(name: &str) -> CodeViewFile<Note> {
    let line = |index: usize, changed: bool| {
        if changed {
            format!("let {name}_{index} = changed({index});")
        } else {
            format!("let {name}_{index} = {index};")
        }
    };
    let text = |changed_lines: bool| {
        (1..=LINES)
            .map(|index| line(index, changed_lines && index % 40 == 0))
            .collect::<Vec<_>>()
            .join("\n")
    };
    CodeViewFile::from_contents(name, file(name, text(false)), file(name, text(true)))
        .expect("two different files have a diff")
}

/// Five files, the first with an annotation on line 3 of its additions.
fn files() -> Vec<CodeViewFile<Note>> {
    let mut files: Vec<_> = (0..5)
        .map(|index| changed_file(&format!("f{index}")))
        .collect();
    files[0].annotations = vec![DiffLineAnnotation {
        side: AnnotationSide::Additions,
        line_number: 3,
        metadata: Some("looks right"),
    }];
    files
}

#[derive(Clone, Default)]
struct Shared {
    state: Rc<RefCell<Option<Rc<State<Note>>>>>,
    scroll_to: Rc<Cell<Option<Signal<Option<ScrollRequest>>>>>,
    /// How many times the page component's body ran.
    renders: Rc<Cell<usize>>,
}

impl PartialEq for Shared {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

#[derive(Props, Clone, PartialEq)]
struct PageProps {
    shared: Shared,
    files: Vec<CodeViewFile<Note>>,
    runtime: Runtime,
    environment: Environment,
}

#[component]
fn Page(props: PageProps) -> Element {
    let scroll_to = use_signal(|| None::<ScrollRequest>);
    props.shared.scroll_to.set(Some(scroll_to));
    let state = use_code_view(&CodeViewViewProps::<Note> {
        files: props.files.clone(),
        height: format!("{VIEWPORT}px"),
        runtime: props.runtime.clone(),
        environment: props.environment.clone(),
        diff_style: DiffStyle::Unified,
        expand_unchanged: true,
        render_annotation: Some(Callback::new(|slot: FileAnnotation<Note>| {
            rsx! {
                aside { class: "note", "{slot.file}: {slot.annotation.metadata:?}" }
            }
        })),
        highlighter: None,
        layout: None,
        scroll_to: scroll_to(),
    });
    props.shared.renders.set(props.shared.renders.get() + 1);
    *props.shared.state.borrow_mut() = Some(Rc::clone(&state));
    state.element()
}

struct Rig {
    dom: VirtualDom,
    shared: Shared,
    state: Rc<State<Note>>,
    root: Rc<Scripted>,
    clock: Clock,
    /// The embedder's `now`, in milliseconds; a test winds it by hand.
    time: Rc<Cell<f64>>,
    /// Every event listener Dioxus was told to attach, by name.
    listeners: Vec<String>,
}

impl Rig {
    fn new() -> Self {
        Self::with(Environment::default())
    }

    fn with(environment: Environment) -> Self {
        let shared = Shared::default();
        let clock = Clock::default();
        let time = Rc::new(Cell::new(1000.0));
        let now_time = Rc::clone(&time);
        let mut dom = VirtualDom::new_with_props(
            Page,
            PageProps {
                shared: shared.clone(),
                files: files(),
                runtime: Runtime {
                    sleep: PtrEq::new(clock.sleep()),
                    now: PtrEq::new(Rc::new(move || now_time.get())),
                },
                environment,
            },
        );
        let first = dom.rebuild_to_vec();
        let state = shared.state.borrow().clone().expect("the page rendered");
        let mut rig = Self {
            dom,
            shared,
            state,
            root: Rc::default(),
            clock,
            time,
            listeners: Vec::new(),
        };
        rig.record(&first.edits);
        rig
    }

    fn record(&mut self, edits: &[Mutation]) {
        self.listeners
            .extend(edits.iter().filter_map(|mutation| match mutation {
                Mutation::NewEventListener { name, .. } => Some(name.to_string()),
                _ => None,
            }));
    }

    fn host(&self) -> &Rc<DioxusHost> {
        &self.state.host
    }

    fn nodes(&self) -> pierre_diffs::components::code_view::SlabNodes {
        self.state.view.slab_nodes().expect("a mounted viewer")
    }

    /// Runs `events` as one dispatch would (in the runtime, so handlers can spawn), then lets
    /// spawned tasks run and Dioxus apply the document's edits.
    fn deliver(&mut self, events: impl FnOnce(&Rig)) {
        self.dom.in_scope(ScopeId::ROOT, || events(self));
        self.settle();
    }

    /// Renders what is dirty, lets spawned tasks run (a render may spawn the paced flush), and
    /// renders what they edited.
    fn settle(&mut self) {
        let mutations = self.dom.render_immediate_to_vec();
        self.record(&mutations.edits);
        run_tasks(&mut self.dom);
        let mutations = self.dom.render_immediate_to_vec();
        self.record(&mutations.edits);
    }

    /// What a webview reports once the page is up: the root mounted and sized.
    fn boot(&mut self) {
        let root = self.host().root();
        let scripted = Rc::clone(&self.root);
        self.deliver(|rig| {
            rig.state
                .dispatch
                .mounted(root, MountedData::new(ScriptedElement(scripted)));
            rig.state.dispatch.resized(root, VIEWPORT, 400.0);
        });
    }

    /// The scroller moved to `top`: the engine's `scroll` event.
    fn scroll_to(&mut self, top: f64) {
        let root = self.host().root();
        let height = self.state.view.get_scroll_height();
        self.root.offset.set((0.0, top));
        self.deliver(|rig| {
            rig.state.dispatch.scrolled(
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

    fn request(&mut self, request: ScrollRequest) {
        let mut signal = self.shared.scroll_to.get().expect("the page rendered");
        self.dom.in_runtime(|| signal.set(Some(request)));
        self.settle();
    }

    /// One frame of the embedder's source: the 16 ms sleeper fires and the clock moves on.
    fn frame(&mut self) -> bool {
        let index = self
            .clock
            .0
            .borrow()
            .iter()
            .position(|(duration, _)| *duration == Duration::from_millis(16));
        let Some(index) = index else {
            return false;
        };
        self.time.set(self.time.get() + 16.0);
        self.clock.fire(index);
        self.settle();
        true
    }

    /// Frames until the viewer asks for no more.
    fn frames_until_idle(&mut self) -> usize {
        let mut frames = 0;
        while self.frame() {
            frames += 1;
            assert!(frames < 400, "the viewer never went idle");
        }
        frames
    }

    /// The shells the slab holds, in order.
    fn shells(&self) -> Vec<NodeKey> {
        self.host().document().children(self.nodes().slab)
    }

    fn page(&self) -> String {
        dioxus_ssr::render(&self.dom)
    }
}

#[test]
fn many_files_mount_only_the_window_with_their_annotations_in_the_slab() {
    let mut rig = Rig::new();
    rig.boot();
    // The first file is 400 lines (8000 px): the window holds it alone, not the other four.
    assert_eq!(rig.shells().len(), 1);
    let page = rig.page();
    assert!(
        page.contains("f0_1 = 1"),
        "{}",
        &page[..page.len().min(400)]
    );
    assert!(!page.contains("f1_1 = 1"));
    // The annotation is the caller's element, placed under its line.
    assert!(page.contains("f0: Some(&#34;looks right&#34;)"));
    // Unified: Pierre calls the layout `single`.
    assert!(page.contains("data-diff-type=\"single\""));
    // The three wrappers carry what the viewer wrote through the host.
    let slab = *rig.state.slab.peek();
    assert!(slab.container_height.is_some_and(|height| height > 8000.0));
    assert!(slab.top.is_some() && slab.bottom.is_some());
    assert!(page.contains(&format!(
        "height:{}px",
        slab.container_height.expect("written")
    )));
}

#[test]
fn a_slab_write_patches_three_styles_and_runs_no_render_of_the_page() {
    let mut rig = Rig::new();
    rig.boot();
    let renders = rig.shared.renders.get();
    let nodes = rig.nodes();
    let before = rig.page();
    rig.dom.in_scope(ScopeId::ROOT, || {
        let host = rig.host();
        host.write_slab_sticky(nodes.slab, -123.0, -45.0);
        host.write_slab_spacer(nodes.spacer, Some(321.0));
        host.write_container_height(nodes.container, 9876.0);
    });
    let mutations = rig.dom.render_immediate_to_vec();
    let styles: Vec<_> = mutations
        .edits
        .iter()
        .filter_map(|mutation| match mutation {
            Mutation::SetAttribute {
                name,
                value: AttributeValue::Text(text),
                ..
            } if name == "style" => Some(text.clone()),
            _ => None,
        })
        .collect();
    // Three attribute writes (each is a push of the element, the write and a pop) and nothing
    // else: a scrolled frame touches the three wrappers and no shell.
    assert!(
        mutations.edits.iter().all(|mutation| matches!(
            mutation,
            Mutation::PushId { .. } | Mutation::SetAttribute { .. } | Mutation::Pop
        )) && styles.len() == 3,
        "a slab write touched more than the three wrappers: {:?}",
        mutations.edits
    );
    assert!(styles.iter().any(|style| style.ends_with("height:9876px;")));
    assert!(styles.iter().any(|style| style.ends_with("height:321px;")));
    assert!(
        styles
            .iter()
            .any(|style| style.ends_with("top:-123px;bottom:-45px;"))
    );
    assert_eq!(rig.shared.renders.get(), renders, "the page re-ran");
    // The `Document` never saw them: the port reads those properties nowhere.
    let document = rig.host().document();
    assert_eq!(document.get_style_property(nodes.slab, "top"), "");
    assert_eq!(document.get_style_property(nodes.container, "height"), "");
    assert_ne!(rig.page(), before);
    // Writing what is already there notifies nobody.
    rig.dom.in_scope(ScopeId::ROOT, || {
        rig.host().write_container_height(nodes.container, 9876.0);
    });
    assert!(rig.dom.render_immediate_to_vec().edits.is_empty());
}

#[test]
fn a_scroll_burst_marks_the_slab_without_rebuilding_the_shells() {
    let mut rig = Rig::new();
    rig.boot();
    let renders = rig.shared.renders.get();
    let slab = rig.nodes().slab;
    // The root's `scroll` event suspends pointer interaction: a `Document` write on the slab.
    // (Called on the viewer, not through the dispatch, so no pass runs behind it: the pass of a
    // scrolled frame edits the rows of the shells, which is a structure edit and does rebuild.)
    rig.deliver(|rig| rig.state.view.handle_scroll());
    assert!(
        rig.page().contains(SUSPENDED_SLAB),
        "the slab did not render the pointer-events write"
    );
    assert_eq!(
        rig.host()
            .document()
            .get_style_property(slab, "pointer-events"),
        "none"
    );
    // The write reached the wrapper by its own signal; the shell list was not rebuilt for it.
    assert_eq!(
        rig.shared.renders.get(),
        renders,
        "the page re-ran for a style write"
    );
    // 120 ms later the viewer restores it.
    let index = rig
        .clock
        .0
        .borrow()
        .iter()
        .position(|(duration, _)| *duration == Duration::from_millis(120))
        .expect("the restore timer is pending");
    rig.clock.fire(index);
    rig.settle();
    // (The timer's dispatch also ran the pass `handle_scroll` queued, which edits rows: a
    // structure edit that does re-render the page, so the render count is not asserted here.)
    assert!(!rig.page().contains(SUSPENDED_SLAB));
}

#[test]
fn scrolling_far_swaps_the_shells_and_the_slab_follows() {
    let mut rig = Rig::new();
    rig.boot();
    let first = rig.shells();
    rig.scroll_to(30_000.0);
    rig.frames_until_idle();
    let shells = rig.shells();
    assert!(!shells.is_empty());
    let page = rig.page();
    assert!(
        page.contains("f3_") && !page.contains("f0_1 = 1"),
        "{shells:?} {first:?}"
    );
    // The spacer sits where the rendered window starts.
    assert!(
        rig.state
            .slab
            .peek()
            .spacer_height
            .is_some_and(|height| height > 24_000.0)
    );
}

#[test]
fn revealing_a_line_scrolls_the_root_to_it() {
    let mut rig = Rig::new();
    rig.boot();
    let top = rig
        .state
        .view
        .get_top_for_item("f2")
        .expect("f2 is a file of the viewer");
    rig.request(ScrollRequest {
        target: CodeViewScrollTarget::Line(CodeViewLineScrollTarget {
            id: "f2".to_owned(),
            line_number: 120,
            side: Some(SelectionSide::Additions),
            align: Some(ScrollAlign::Start),
            offset: None,
            behavior: Some(CodeViewScrollBehavior::Instant),
        }),
        serial: 1,
    });
    rig.frames_until_idle();
    let writes: Vec<f64> = rig
        .root
        .log
        .borrow()
        .iter()
        .filter_map(|entry| entry.strip_prefix("scroll 0 "))
        .map(|rest| {
            rest.split_whitespace()
                .next()
                .expect("a position")
                .parse()
                .expect("a number")
        })
        .collect();
    let landed = *writes.last().expect("the reveal wrote a scroll position");
    // Line 120 is 119 rows of 20 px below the file's first row.
    assert!(
        (top + 119.0 * 20.0..=top + 119.0 * 20.0 + 150.0).contains(&landed),
        "landed at {landed}, f2 starts at {top}"
    );
    assert!(rig.page().contains("f2_125 = 125"));
}

#[test]
fn a_smooth_reveal_steps_one_spring_position_per_frame_on_the_embedders_clock() {
    let mut rig = Rig::new();
    rig.boot();
    let top = rig.state.view.get_top_for_item("f0").expect("f0");
    rig.request(ScrollRequest {
        target: CodeViewScrollTarget::Line(CodeViewLineScrollTarget {
            id: "f0".to_owned(),
            line_number: 100,
            side: Some(SelectionSide::Additions),
            align: Some(ScrollAlign::Start),
            offset: None,
            behavior: Some(CodeViewScrollBehavior::Smooth),
        }),
        serial: 1,
    });
    let frames = rig.frames_until_idle();
    let writes: Vec<f64> = rig
        .root
        .log
        .borrow()
        .iter()
        .filter_map(|entry| entry.strip_prefix("scroll 0 "))
        .map(|rest| {
            rest.split_whitespace()
                .next()
                .expect("a position")
                .parse()
                .expect("a number")
        })
        .collect();
    assert!(
        frames > 3 && writes.len() > 3,
        "{frames} frames, writes {writes:?}"
    );
    assert!(
        writes.windows(2).all(|pair| pair[0] <= pair[1] + 0.5),
        "the spring went backwards: {writes:?}"
    );
    let landed = *writes.last().expect("writes");
    assert!(
        (landed - (top + 99.0 * 20.0)).abs() < 150.0,
        "landed at {landed}"
    );
    // The frame pacing: no more than one pass per 16 ms the clock moved.
    assert!(rig.time.get() >= 1000.0 + 16.0 * frames as f64);
}

#[test]
fn a_reveal_request_acts_once_until_its_serial_changes() {
    let mut rig = Rig::new();
    rig.boot();
    let target = |serial| ScrollRequest::to_line(serial, "f1", SelectionSide::Additions, 40);
    rig.request(target(1));
    rig.frames_until_idle();
    let writes = rig.root.log.borrow().len();
    assert!(writes > 0);
    // The same request again (a re-render with equal props) does nothing...
    rig.request(target(1));
    rig.frames_until_idle();
    assert_eq!(rig.root.log.borrow().len(), writes);
    // ... and a new serial for the same line does, after the person has scrolled away.
    rig.scroll_to(0.0);
    rig.frames_until_idle();
    rig.request(target(2));
    rig.frames_until_idle();
    assert!(rig.root.log.borrow().len() > writes);
}

#[test]
fn user_input_on_the_root_ends_a_programmatic_scroll() {
    let mut rig = Rig::new();
    rig.boot();
    rig.request(ScrollRequest::to_line(
        1,
        "f4",
        SelectionSide::Additions,
        40,
    ));
    // A person's wheel before the frame runs takes the scroll over.
    rig.deliver(|rig| rig.state.dispatch.took_over());
    rig.frames_until_idle();
    assert!(
        rig.root.log.borrow().is_empty(),
        "the pending reveal still wrote: {:?}",
        rig.root.log.borrow()
    );
}

#[test]
fn the_root_renders_with_the_listeners_the_viewer_registered() {
    let mut rig = Rig::new();
    rig.boot();
    let listeners = &rig.listeners;
    // The root listens for what the viewer registered, the shells for focus.
    for expected in [
        "focusin",
        "focusout",
        "keydown",
        "mounted",
        "pointerdown",
        "resize",
        "scroll",
        "touchstart",
        "wheel",
    ] {
        assert!(
            listeners.iter().any(|name| name == expected),
            "no {expected} listener in {listeners:?}"
        );
    }
    // Only the root scrolls and takes input, and the view is a single scroller.
    assert_eq!(listeners.iter().filter(|name| *name == "wheel").count(), 1);
    assert_eq!(listeners.iter().filter(|name| *name == "scroll").count(), 1);
    // No visibility observer: the viewer renders by scroll position.
    assert!(!listeners.iter().any(|name| name == "visible"));
}

#[test]
fn a_shell_reports_the_focus_inside_it() {
    let mut rig = Rig::new();
    rig.boot();
    let shell = rig.shells()[0];
    assert_eq!(rig.host().active_element(), None);
    rig.deliver(|rig| {
        rig.host()
            .code_view()
            .expect("a code view host")
            .record_focus(shell, true)
    });
    assert_eq!(rig.host().active_element(), Some(shell));
    rig.deliver(|rig| {
        rig.host()
            .code_view()
            .expect("a code view host")
            .record_focus(shell, false)
    });
    assert_eq!(rig.host().active_element(), None);
}

#[test]
fn files_changed_by_props_keep_their_versions_unless_they_changed() {
    let files = files();
    let mut applied = Vec::new();
    let items = State::<Note>::next_items(&mut applied, &files).expect("a first list");
    assert_eq!(items.len(), 5);
    assert_eq!(
        applied.iter().map(|file| file.version).collect::<Vec<_>>(),
        [0; 5]
    );
    // The same props: nothing to hand the viewer.
    assert!(State::<Note>::next_items(&mut applied, &files).is_none());
    // One annotation added: that file's version moves, the others stay, and the unchanged
    // files keep the identity of their annotation lists.
    let kept = Rc::clone(&applied[2].annotations);
    let mut next = files.clone();
    next[1] = next[1].clone().with_annotations(vec![DiffLineAnnotation {
        side: AnnotationSide::Deletions,
        line_number: 3,
        metadata: Some("x"),
    }]);
    assert!(State::<Note>::next_items(&mut applied, &next).is_some());
    assert_eq!(
        applied.iter().map(|file| file.version).collect::<Vec<_>>(),
        [0, 1, 0, 0, 0]
    );
    assert!(Rc::ptr_eq(&kept, &applied[2].annotations));
    // Collapsing keeps the annotation list's identity and bumps the version again.
    let list = Rc::clone(&applied[1].annotations);
    next[1] = next[1].clone().with_collapsed(true);
    assert!(State::<Note>::next_items(&mut applied, &next).is_some());
    assert_eq!(applied[1].version, 2);
    assert!(Rc::ptr_eq(&list, &applied[1].annotations));
    // Dropping a file and reordering restarts nothing that stayed.
    next.remove(0);
    next.reverse();
    assert!(State::<Note>::next_items(&mut applied, &next).is_some());
    assert_eq!(
        applied
            .iter()
            .map(|file| (file.id.as_str(), file.version))
            .collect::<Vec<_>>(),
        [("f4", 0), ("f3", 0), ("f2", 0), ("f1", 2)]
    );
}

#[test]
fn files_come_from_patches_and_contents_and_errors_are_the_callers() {
    let patch = "--- x.txt\n+++ x.txt\n@@ -1,2 +1,2 @@\n one\n-two\n+three\n\
                 --- y.txt\n+++ y.txt\n@@ -1 +1 @@\n-a\n+b\n\
                 --- x.txt\n+++ x.txt\n@@ -1 +1 @@\n-c\n+d\n";
    let files = CodeViewFile::<Note>::from_patch(patch).expect("a well-formed patch");
    assert_eq!(
        files
            .iter()
            .map(|file| file.id.as_str())
            .collect::<Vec<_>>(),
        ["x.txt", "y.txt", "x.txt#2"]
    );
    assert!(CodeViewFile::<Note>::from_patch("not a patch").is_err());
    let one = CodeViewFile::<Note>::from_contents(
        "a",
        file("a.rs", "fn main() {}\n".to_owned()),
        file("a.rs", "fn main() { run(); }\n".to_owned()),
    )
    .expect("two different files have a diff");
    assert_eq!(one.id, "a");
}

// The host on its own: what the viewer asks of its platform, without a view.

struct HostRig {
    dom: VirtualDom,
    host: Rc<DioxusHost>,
    slab: Signal<SlabStyle>,
    time: Rc<Cell<f64>>,
    clock: Clock,
}

#[component]
fn Empty() -> Element {
    rsx! {}
}

impl HostRig {
    fn new(environment: Environment) -> Self {
        let mut dom = VirtualDom::new(Empty);
        dom.rebuild_in_place();
        let time = Rc::new(Cell::new(5.0));
        let now_time = Rc::clone(&time);
        let clock = Clock::default();
        let (host, slab) = dom.in_scope(ScopeId::ROOT, || {
            let slab = Signal::new(SlabStyle::default());
            let host = DioxusHost::new_code_view(
                "100px",
                clock.sleep(),
                Rc::new(move || now_time.get()),
                environment,
                slab,
                20.0,
            );
            (host, slab)
        });
        Self {
            dom,
            host,
            slab,
            time,
            clock,
        }
    }
}

#[test]
fn the_host_answers_the_platform_reads_from_the_embedder() {
    let environment = Environment {
        device_pixel_ratio: 2.5,
        prefers_reduced_motion: true,
        platform: PlatformInfo {
            user_agent: "Mozilla/5.0 Gecko/20100101 Firefox/130.0".to_owned(),
            max_touch_points: 5,
            ..PlatformInfo::default()
        },
    };
    let rig = HostRig::new(environment.clone());
    assert_eq!(rig.host.device_pixel_ratio(), 2.5);
    assert!(rig.host.prefers_reduced_motion());
    assert_eq!(rig.host.platform_info(), environment.platform);
    // Read fresh: a zoom is seen.
    rig.host
        .code_view()
        .expect("a code view host")
        .set_environment(Environment::default());
    assert_eq!(rig.host.device_pixel_ratio(), 1.0);
    assert!(!rig.host.prefers_reduced_motion());
    assert_eq!(rig.host.now(), 5.0);
    rig.time.set(77.5);
    assert_eq!(rig.host.now(), 77.5);
}

#[test]
fn the_slab_jitter_is_in_range_and_not_constant() {
    let rig = HostRig::new(Environment::default());
    let draws: Vec<f64> = (0..200).map(|_| rig.host.random()).collect();
    assert!(draws.iter().all(|draw| (0.0..1.0).contains(draw)));
    let distinct: HashSet<u64> = draws.iter().map(|draw| draw.to_bits()).collect();
    assert_eq!(distinct.len(), draws.len());
    let mean = draws.iter().sum::<f64>() / draws.len() as f64;
    assert!((0.4..0.6).contains(&mean), "mean {mean}");
}

#[test]
fn registering_the_shell_listeners_and_observer_marks_the_nodes_the_renderer_decorates() {
    let rig = HostRig::new(Environment::default());
    let host = &rig.host;
    let root = host.root();
    let other = host.document().create_element("div");
    assert!(!host.wants_resize(other) && !host.wants_scroll(root));
    host.shell_observe(&other);
    assert!(
        host.wants_resize(other)
            && host
                .code_view()
                .expect("a code view host")
                .observes_shell(other)
    );
    host.shell_unobserve(&other);
    assert!(!host.wants_resize(other));
    host.shell_observe(&other);
    host.shell_disconnect();
    assert!(
        !host
            .code_view()
            .expect("a code view host")
            .observes_shell(other)
    );
    host.add_shell_listeners(&root);
    assert!(
        host.wants_scroll(root)
            && host
                .code_view()
                .expect("a code view host")
                .wants_shell_listeners(root)
    );
    assert_eq!(host.scroll_side(root), Some(None));
    host.remove_shell_listeners(&root);
    assert!(
        !host.wants_scroll(root)
            && !host
                .code_view()
                .expect("a code view host")
                .wants_shell_listeners(root)
    );
}

#[test]
fn the_slab_writes_set_the_signal_and_leave_the_document_alone() {
    let rig = HostRig::new(Environment::default());
    let document = rig.host.document();
    let (container, spacer, slab) = (
        document.create_element("div"),
        document.create_element("div"),
        document.create_element("div"),
    );
    let edits = Rc::new(Cell::new(0));
    let counter = Rc::clone(&edits);
    document.set_observer(Some(Rc::new(move |_, _| counter.set(counter.get() + 1))));
    rig.dom.in_scope(ScopeId::ROOT, || {
        rig.host.write_container_height(container, 500.0);
        rig.host.write_slab_spacer(spacer, Some(40.0));
        rig.host.write_slab_sticky(slab, -10.0, 30.0);
    });
    assert_eq!(edits.get(), 0, "a slab write edited the document");
    assert_eq!(
        *rig.slab.peek(),
        SlabStyle {
            container_height: Some(500.0),
            spacer_height: Some(40.0),
            top: Some(-10.0),
            bottom: Some(30.0),
            revision: 0,
        }
    );
    rig.dom
        .in_scope(ScopeId::ROOT, || rig.host.write_slab_spacer(spacer, None));
    assert_eq!(rig.slab.peek().spacer_height, None);
    assert_eq!(
        SlabStyle {
            spacer_height: Some(-0.0),
            ..SlabStyle::default()
        }
        .declarations(SlabRole::Spacer),
        "height:0px;"
    );
}

#[test]
fn frames_wait_for_the_root_then_carry_the_embedders_clock_and_a_follow_up_is_paced() {
    let mut rig = HostRig::new(Environment::default());
    let host = Rc::clone(&rig.host);
    let stamps = Rc::new(RefCell::new(Vec::new()));
    let stamp = Rc::clone(&stamps);
    let follow_up = Rc::clone(&host);
    let follow_stamp = Rc::clone(&stamps);
    host.request_animation_frame(Box::new(move |timestamp| {
        stamp.borrow_mut().push(timestamp);
        // The pass that still has work requeues itself.
        follow_up.request_animation_frame(Box::new(move |timestamp| {
            follow_stamp.borrow_mut().push(timestamp);
        }));
    }));
    rig.dom.in_scope(ScopeId::ROOT, || host.flush_frames());
    assert!(
        stamps.borrow().is_empty(),
        "a frame ran before the root was mounted"
    );
    rig.time.set(40.0);
    let root = host.root();
    rig.dom.in_scope(ScopeId::ROOT, || {
        host.record_mounted(
            root,
            Rc::new(MountedData::new(ScriptedElement(Rc::default()))),
        );
        host.flush_frames();
    });
    assert_eq!(*stamps.borrow(), [40.0]);
    // The follow-up waits for a frame interval of the embedder's sleep, not for the next tick.
    run_tasks(&mut rig.dom);
    assert_eq!(stamps.borrow().len(), 1);
    assert_eq!(rig.clock.0.borrow().len(), 1);
    rig.time.set(56.0);
    assert_eq!(rig.clock.fire(0), Duration::from_millis(16));
    run_tasks(&mut rig.dom);
    assert_eq!(*stamps.borrow(), [40.0, 56.0]);
}
