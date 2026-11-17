//! `CodeViewView`: Pierre's `CodeView` (many files in one virtualized scroller) as an ordinary
//! Dioxus component, over the `pierre_diffs` port.
//!
//! The component owns a `CodeView` that builds its DOM in a `Document` (see `host.rs`), and
//! renders that document with `render.rs`, as `FileDiffView` does for one file. What differs:
//!
//! * The viewer decides what is mounted from the scroll position, so there are no visibility
//!   events. Geometry comes from `onresize` and `onscroll`, scroll writes go through
//!   `MountedData`, frames are paced by the embedder's `sleep` on its `now` clock (`Runtime`).
//! * The viewer's slab moves on every scrolled frame, which must not rebuild the shells it
//!   holds. Its three per-frame writes set a `Signal` that three wrapper components render their
//!   `style` from (`render.rs`, `host/code_view.rs`); the `Document`'s structure edits (a shell
//!   mounted, released or reordered) re-render this component, a scroll does not.
//! * Files are controlled: `CodeViewViewProps::files` is the whole list, and the viewer keeps
//!   the records, instances and measured heights of the files whose entry did not change. The
//!   viewer takes item updates by version number; the component numbers them, bumping a file's
//!   when its diff, annotations or `collapsed` changed.
//!
//! View-only: the pointer does nothing yet (no hover, selection or hunk expansion), and the
//! header and footer slots of the viewer are not exposed.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use dioxus::prelude::*;
use pierre_diffs::components::code_view::CodeView;
use pierre_diffs::components::code_view::options::CodeViewDiffItemContext;
use pierre_diffs::components::code_view::options::CodeViewOptions;
use pierre_diffs::document::Mutation;
use pierre_diffs::highlighter_types::DiffsHighlighter;
use pierre_diffs::highlighter_types::PlainTextHighlighter;
use pierre_diffs::js_collections::PtrEq;
use pierre_diffs::managers::resize_manager::ResizeObserverHub;
use pierre_diffs::managers::universal_rendering_manager::UniversalRenderingManager;
use pierre_diffs::types::layout::CodeViewLayout;
use pierre_diffs::types::options::DiffStyle;
use pierre_diffs::types::patch::DiffFileInput;
use pierre_diffs::types::patch::FileContents;
use pierre_diffs::types::patch::FileDiffMetadata;
use pierre_diffs::types::patch::ParseDiffOptions;
use pierre_diffs::types::patch::ParsePatchOptions;
use pierre_diffs::types::view::CodeViewDiffItem;
use pierre_diffs::types::view::CodeViewItem;
use pierre_diffs::types::view::CodeViewLineScrollTarget;
use pierre_diffs::types::view::CodeViewScrollTarget;
use pierre_diffs::types::view::DiffLineAnnotation;
use pierre_diffs::types::view::SelectionSide;
use pierre_diffs::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use pierre_diffs::utils::compute_virtual_file_metrics::compute_virtual_file_metrics;
use pierre_diffs::utils::parse_diff_from_file::parse_diff_from_file;
use pierre_diffs::utils::parse_patch_files::ParsePatchError;
use pierre_diffs::utils::parse_patch_files::parse_patch_files;

use crate::events::CodeViewManagers;
use crate::events::Dispatch;
use crate::events::ShellEvents;
use crate::host::CodeViewHostState;
use crate::host::DioxusHost;
use crate::host::Environment;
use crate::host::SlabStyle;
use crate::host::SleepFuture;
use crate::render::Renderer;
use crate::slot::SlotContent;
use crate::slot::SlotTable;

/// One file of the viewer: a diff, the annotations placed on it and whether it is collapsed.
#[derive(Clone)]
pub struct CodeViewFile<A> {
    /// Names the file to the viewer and to a scroll target; unique in the list.
    pub id: String,
    /// The parsed diff, compared by identity as the port compares diff targets: keep the same
    /// `Rc` across renders for a file that did not change.
    pub diff: Rc<FileDiffMetadata>,
    /// Compared by value; an equal list is the same list.
    pub annotations: Vec<DiffLineAnnotation<A>>,
    /// Only the file's header shows.
    pub collapsed: bool,
}

impl<A> CodeViewFile<A> {
    /// A file over an already parsed diff, with no annotations.
    pub fn new(id: impl Into<String>, diff: Rc<FileDiffMetadata>) -> Self {
        Self {
            id: id.into(),
            diff,
            annotations: Vec::new(),
            collapsed: false,
        }
    }

    /// The diff of two versions of one file (the port computes it). The files come from
    /// outside the program, so a failure to parse the patch computed from them is returned for
    /// the caller to handle at its edge.
    pub fn from_contents(
        id: impl Into<String>,
        old_file: FileContents,
        new_file: FileContents,
    ) -> Result<Self, ParsePatchError> {
        let parsed = parse_diff_from_file(
            &DiffFileInput::Both { old_file, new_file },
            &ParseDiffOptions::default(),
            true,
        )?;
        Ok(Self::new(id, Rc::new(parsed.value)))
    }

    /// Every file of a unified or git patch, each named by its path (a path that repeats gets
    /// `#2`, `#3` ... appended, so ids stay unique). A malformed patch, or one with no file in it,
    /// is returned for the caller to handle at its edge.
    pub fn from_patch(patch: &str) -> Result<Vec<Self>, ParsePatchError> {
        let options = ParsePatchOptions {
            throw_on_error: Some(true),
            ..ParsePatchOptions::default()
        };
        let parsed = parse_patch_files(patch, &options)?;
        let mut seen: HashMap<String, usize> = HashMap::new();
        let files: Vec<Self> = parsed
            .value
            .into_iter()
            .flat_map(|patch| patch.files)
            .map(|diff| {
                let count = seen.entry(diff.name.clone()).or_default();
                *count += 1;
                let id = match *count {
                    1 => diff.name.clone(),
                    count => format!("{}#{count}", diff.name),
                };
                Self::new(id, Rc::new(diff))
            })
            .collect();
        if files.is_empty() {
            return Err(ParsePatchError {
                message: "patch contains no file diff".to_owned(),
            });
        }
        Ok(files)
    }

    pub fn with_annotations(mut self, annotations: Vec<DiffLineAnnotation<A>>) -> Self {
        self.annotations = annotations;
        self
    }

    pub fn with_collapsed(mut self, collapsed: bool) -> Self {
        self.collapsed = collapsed;
        self
    }
}

impl<A: PartialEq> PartialEq for CodeViewFile<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && Rc::ptr_eq(&self.diff, &other.diff)
            && self.collapsed == other.collapsed
            && self.annotations == other.annotations
    }
}

/// An annotation and the file it sits in: what `render_annotation` is asked for.
#[derive(Clone, PartialEq)]
pub struct FileAnnotation<A> {
    /// The `CodeViewFile::id` of the file.
    pub file: String,
    pub annotation: DiffLineAnnotation<A>,
}

/// A request to scroll the viewer: the target, and a serial that tells a repeat of the same
/// target from the same request seen again. The viewer acts when the request differs from the
/// one it last saw, so to scroll to the same line a second time, send it with a new serial.
#[derive(Clone, PartialEq)]
pub struct ScrollRequest {
    pub target: CodeViewScrollTarget,
    pub serial: u64,
}

impl ScrollRequest {
    /// Brings `line_number` of `side` of the file named `id` into view (the viewer's default
    /// alignment: the nearest edge, and the smooth scroll for a short way).
    pub fn to_line(
        serial: u64,
        id: impl Into<String>,
        side: SelectionSide,
        line_number: usize,
    ) -> Self {
        Self {
            target: CodeViewScrollTarget::Line(CodeViewLineScrollTarget {
                id: id.into(),
                line_number,
                side: Some(side),
                align: None,
                offset: None,
                behavior: None,
            }),
            serial,
        }
    }
}

/// What the viewer cannot get from standard Dioxus: a timer and a clock. Read when the view
/// mounts.
#[derive(Clone, PartialEq)]
pub struct Runtime {
    /// A future that resolves after the duration (`tokio::time::sleep` on desktop, the
    /// browser's timer on the web). Paces the viewer's frames, a frame being 16 ms.
    pub sleep: PtrEq<dyn Fn(Duration) -> SleepFuture>,
    /// A monotonic clock in milliseconds (`performance.now()` on the web, time since start
    /// elsewhere): frame timestamps and the start of a smooth scroll are on it.
    pub now: PtrEq<dyn Fn() -> f64>,
}

#[derive(Props, Clone, PartialEq)]
pub struct CodeViewViewProps<A: Clone + PartialEq + 'static> {
    /// Every file, in order.
    pub files: Vec<CodeViewFile<A>>,
    /// The scroller's CSS `height` (`"40rem"`, `"100%"` of a bounded parent): the one thing a
    /// virtualizer cannot derive, as a scroller that grows with its content has nothing to
    /// virtualize.
    pub height: String,
    pub runtime: Runtime,
    /// Device pixel ratio, reduced motion and the platform strings; the defaults are a plain
    /// desktop.
    #[props(default)]
    pub environment: Environment,
    /// Unified or split; Pierre's default is split.
    #[props(default = DiffStyle::Split)]
    pub diff_style: DiffStyle,
    /// Show the unchanged lines between hunks instead of collapsing them behind separators.
    /// (The separators do not expand on click yet: the view is view-only.)
    #[props(default)]
    pub expand_unchanged: bool,
    /// Content for an annotation of any file, placed under its line. `FileDiff` asks once per
    /// annotation and keeps the placement; the callback itself runs on every render of this
    /// component, so it may read signals.
    #[props(default)]
    pub render_annotation: Option<Callback<FileAnnotation<A>, Element>>,
    /// The tokenizer behind syntax highlighting; plain text when absent. Read when the view
    /// mounts: a different highlighter needs a new view.
    #[props(default)]
    pub highlighter: Option<PtrEq<dyn DiffsHighlighter>>,
    /// The paddings of the scroll content and the gap between files; the viewer's own default
    /// when absent.
    #[props(default)]
    pub layout: Option<CodeViewLayout>,
    /// Scrolls to a line, range, file or position when the request changes.
    #[props(default)]
    pub scroll_to: Option<ScrollRequest>,
}

/// The props that become `CodeViewOptions`; a change needs `set_options`.
#[derive(Clone, Copy, PartialEq)]
struct ViewOptions {
    diff_style: DiffStyle,
    expand_unchanged: bool,
    layout: Option<CodeViewLayout>,
}

impl ViewOptions {
    fn of<A: Clone + PartialEq + 'static>(props: &CodeViewViewProps<A>) -> Self {
        Self {
            diff_style: props.diff_style,
            expand_unchanged: props.expand_unchanged,
            layout: props.layout,
        }
    }
}

type AnnotationCallback<A> = Rc<RefCell<Option<Callback<FileAnnotation<A>, Element>>>>;

fn code_view_options<A: Clone + PartialEq + 'static>(
    view_options: ViewOptions,
    slots: &Rc<SlotTable>,
    render_annotation: &AnnotationCallback<A>,
) -> CodeViewOptions<SlotContent, A> {
    let mut options = CodeViewOptions::default();
    options.diff.base.diff_style = Some(view_options.diff_style);
    options.diff.base.expand_unchanged = Some(view_options.expand_unchanged);
    options.layout = view_options.layout;
    let slots = Rc::clone(slots);
    let render_annotation = Rc::clone(render_annotation);
    options.callbacks.render_annotation = Some(PtrEq::new(Rc::new(
        move |annotation: &DiffLineAnnotation<A>,
              _diff: &FileDiffMetadata,
              context: &CodeViewDiffItemContext<A>| {
            // `Callback` is `Copy`, so the borrow ends before the content is made.
            let callback = (*render_annotation.borrow())?;
            let annotation = FileAnnotation {
                file: context.item.id.clone(),
                annotation: annotation.clone(),
            };
            Some(SlotContent::new(&slots, move || {
                callback.call(annotation.clone())
            }))
        },
    )));
    options
}

/// What was last handed to the viewer for a file, and the version it went in under.
struct AppliedFile<A> {
    id: String,
    diff: Rc<FileDiffMetadata>,
    annotations: Rc<Vec<DiffLineAnnotation<A>>>,
    collapsed: bool,
    version: usize,
}

struct Applied<A> {
    files: Vec<AppliedFile<A>>,
    options: ViewOptions,
    height: String,
    environment: Environment,
    scroll_to: Option<ScrollRequest>,
}

struct State<A: Clone + PartialEq + 'static> {
    host: Rc<DioxusHost>,
    view: Rc<CodeView<DioxusHost, SlotContent, A>>,
    dispatch: Rc<Dispatch>,
    slots: Rc<SlotTable>,
    /// The caller's current callback, read when an annotation's content is made.
    render_annotation: AnnotationCallback<A>,
    slab: Signal<SlabStyle>,
    /// True while `sync` runs: the render that follows reads the document, so edits made now
    /// need no signal (and writing one during the render that reads it only schedules a second,
    /// identical render).
    syncing: Rc<Cell<bool>>,
    applied: RefCell<Applied<A>>,
}

impl<A: Clone + PartialEq + 'static> State<A> {
    fn new(props: &CodeViewViewProps<A>, version: Signal<u64>, slab: Signal<SlabStyle>) -> Self {
        let slots = Rc::new(SlotTable::default());
        let render_annotation: AnnotationCallback<A> = Rc::default();
        let highlighter: Rc<dyn DiffsHighlighter> = match &props.highlighter {
            Some(highlighter) => Rc::clone(&highlighter.0),
            None => Rc::new(PlainTextHighlighter),
        };
        let view_options = ViewOptions::of(props);
        let host = DioxusHost::new_code_view(
            &props.height,
            Rc::clone(&props.runtime.sleep.0),
            Rc::clone(&props.runtime.now.0),
            props.environment.clone(),
            slab,
            compute_virtual_file_metrics(PartialVirtualFileMetrics::default()).line_height,
        );
        let hub = ResizeObserverHub::new(Rc::clone(&host));
        let view = CodeView::new(
            Rc::clone(&host),
            Rc::clone(&hub),
            UniversalRenderingManager::new(Rc::clone(&host)),
            highlighter,
            code_view_options(view_options, &slots, &render_annotation),
        );
        host.code_view()
            .expect("a code view host has its shell state")
            .set_slab_nodes(
                view.slab_nodes()
                    .expect("a viewer that is not cleaned up has its scaffolding"),
            );
        let syncing = Rc::new(Cell::new(true));
        // A `Weak`: the document is the host's, and an observer holding the host would be a cycle.
        let observer_host = Rc::downgrade(&host);
        let observer_syncing = Rc::clone(&syncing);
        host.document()
            .set_observer(Some(Rc::new(move |_document, mutation| {
                if observer_syncing.get() {
                    return;
                }
                let host = observer_host
                    .upgrade()
                    .expect("the host outlives the observer: the view clears it first");
                let shell = host
                    .code_view()
                    .expect("a code view host has its shell state");
                let (Mutation::SetAttribute { node, .. }
                | Mutation::RemoveAttribute { node, .. }
                | Mutation::SetStyleProperty { node, .. }
                | Mutation::RemoveStyleProperty { node, .. }
                | Mutation::EditProperties { node }) = mutation
                else {
                    bump(version);
                    return;
                };
                // The attributes of the slab's three wrappers are read by their own components: a
                // pointer-events or overflow write of a scroll burst must not rebuild the shells.
                if shell.slab_role(*node).is_some() {
                    shell.touch_slab();
                } else {
                    bump(version);
                }
            })));
        let events: Rc<dyn ShellEvents> = Rc::clone(&view) as Rc<dyn ShellEvents>;
        let dispatch = Rc::new(Dispatch {
            host: Rc::clone(&host),
            managers: None,
            code_view: Some(CodeViewManagers { hub, events }),
            batch: RefCell::default(),
        });
        view.setup(host.root());
        syncing.set(false);
        Self {
            host,
            view,
            dispatch,
            slots,
            render_annotation,
            slab,
            syncing,
            applied: RefCell::new(Applied {
                files: Vec::new(),
                options: view_options,
                height: props.height.clone(),
                environment: props.environment.clone(),
                scroll_to: None,
            }),
        }
    }

    fn shell(&self) -> &CodeViewHostState {
        self.host
            .code_view()
            .expect("a code view host has its shell state")
    }

    /// Brings the viewer to `props`. Called from the component body, so the document is current
    /// when the same render reads it.
    fn sync(&self, props: &CodeViewViewProps<A>) {
        *self.render_annotation.borrow_mut() = props.render_annotation;
        self.syncing.set(true);
        let mut applied = self.applied.borrow_mut();
        if applied.environment != props.environment {
            self.shell().set_environment(props.environment.clone());
            applied.environment = props.environment.clone();
        }
        if applied.height != props.height {
            self.host
                .document()
                .set_style_property(self.host.root(), "height", &props.height);
            applied.height = props.height.clone();
        }
        let options = ViewOptions::of(props);
        if applied.options != options {
            self.view.set_options(Some(code_view_options(
                options,
                &self.slots,
                &self.render_annotation,
            )));
            applied.options = options;
        }
        // Set the items before the scroll: a target resolves against the layout of the items.
        if let Some(items) = Self::next_items(&mut applied.files, &props.files) {
            self.view.set_items(&items);
        }
        if applied.scroll_to != props.scroll_to {
            applied.scroll_to = props.scroll_to.clone();
            if let Some(request) = &props.scroll_to {
                self.view.scroll_to(&request.target);
            }
        }
        self.syncing.set(false);
        // What the pass queued runs on a frame, not on this render.
        self.host.schedule_flush();
    }

    /// The viewer's item list for `files`, `None` when it is the one it already has: an entry
    /// whose diff, annotations and `collapsed` are the same keeps its version and an
    /// unchanged entry's annotations keep their identity (the viewer takes an item's update
    /// only when its version moves, and tells a new annotation list by its identity).
    fn next_items(
        applied: &mut Vec<AppliedFile<A>>,
        files: &[CodeViewFile<A>],
    ) -> Option<Vec<CodeViewItem<A>>> {
        let unchanged = applied.len() == files.len()
            && applied.iter().zip(files).all(|(applied, file)| {
                applied.id == file.id
                    && Rc::ptr_eq(&applied.diff, &file.diff)
                    && applied.collapsed == file.collapsed
                    && *applied.annotations == file.annotations
            });
        if unchanged {
            return None;
        }
        let previous: HashMap<&str, &AppliedFile<A>> = applied
            .iter()
            .map(|applied| (applied.id.as_str(), applied))
            .collect();
        let next: Vec<AppliedFile<A>> = files
            .iter()
            .map(|file| match previous.get(file.id.as_str()) {
                Some(applied)
                    if Rc::ptr_eq(&applied.diff, &file.diff)
                        && applied.collapsed == file.collapsed
                        && *applied.annotations == file.annotations =>
                {
                    AppliedFile {
                        id: file.id.clone(),
                        diff: Rc::clone(&file.diff),
                        annotations: Rc::clone(&applied.annotations),
                        collapsed: file.collapsed,
                        version: applied.version,
                    }
                }
                changed => AppliedFile {
                    id: file.id.clone(),
                    diff: Rc::clone(&file.diff),
                    annotations: match changed {
                        Some(applied) if *applied.annotations == file.annotations => {
                            Rc::clone(&applied.annotations)
                        }
                        _ => Rc::new(file.annotations.clone()),
                    },
                    collapsed: file.collapsed,
                    version: changed.map_or(0, |applied| applied.version + 1),
                },
            })
            .collect();
        let items = next
            .iter()
            .map(|file| {
                CodeViewItem::Diff(CodeViewDiffItem {
                    id: file.id.clone(),
                    file_diff: Rc::clone(&file.diff),
                    annotations: Some(Rc::clone(&file.annotations)),
                    version: Some(file.version),
                    collapsed: Some(file.collapsed),
                    edit: None,
                })
            })
            .collect();
        *applied = next;
        Some(items)
    }

    fn element(&self) -> Element {
        let document = self.host.document();
        self.slots.prune(document);
        self.host.prune();
        Renderer {
            document,
            slots: &self.slots,
            dispatch: &self.dispatch,
            pointers: None,
            slab: Some(self.slab),
        }
        .render(self.host.top())
    }

    fn clean_up(&self) {
        self.host.document().set_observer(None);
        self.view.clean_up();
    }
}

/// A signal is `Copy`; the write needs a binding of its own per call.
fn bump(version: Signal<u64>) {
    let mut version = version;
    version += 1;
}

/// The state of this component instance, brought up to `props`.
fn use_code_view<A: Clone + PartialEq + 'static>(props: &CodeViewViewProps<A>) -> Rc<State<A>> {
    let version = use_signal(|| 0_u64);
    let slab = use_signal(SlabStyle::default);
    let state = use_hook(|| Rc::new(State::new(props, version, slab)));
    state.sync(props);
    // Subscribes this render to edits made outside `sync`.
    let _: u64 = version();
    use_drop({
        let state = Rc::clone(&state);
        move || state.clean_up()
    });
    state
}

/// Many files in one virtualized scroller: `CodeView`. The name is the port type's with `View`
/// added, as `FileDiffView` is `FileDiff`'s: the rule maps a Pierre type to its Dioxus component
/// mechanically, and `CodeView` itself is the port's type this component wraps.
#[component]
pub fn CodeViewView<A: Clone + PartialEq + 'static>(props: CodeViewViewProps<A>) -> Element {
    use_code_view(&props).element()
}

#[cfg(test)]
mod tests;
