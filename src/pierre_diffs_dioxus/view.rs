//! `FileDiffView`: a Pierre diff as an ordinary Dioxus component.
//!
//! The component owns a `FileDiff` that renders into a `Document` (see `host.rs`), and renders
//! that document with `render.rs`. It knows nothing about where it runs: no Clay, no JS, only
//! standard Dioxus.
//!
//! How the component learns the document changed. `FileDiff` mutates the document from its own
//! entry points. Two callers reach them:
//! * the component itself, when its props changed (`View::sync`, inside the render that is about
//!   to read the document, so nothing needs to be scheduled), and
//! * anything else later (a handler, a timer, a measurement), which edits the document between
//!   renders. The document's observer bumps a version signal the render reads, so Dioxus
//!   re-renders and the next pass sees the edit.
//!
//! The observer ignores edits made while `View::sync` runs: writing a signal during the render
//! that reads it would only schedule a second, identical render.
//!
//! Interaction (`FileDiffViewProps::interaction`) is the `InteractionManager`'s: line hover,
//! line and range selection, the gutter utility and hunk expansion. Pointer events reach it
//! through `events::Pointers`, from handlers the renderer attaches to the nodes it resolves
//! targets from; its edits (`data-hovered`, `data-selected-line`) reach Dioxus like any other
//! later edit.
//!
//! Virtualized mode (`FileDiffViewProps::virtualized`) swaps the engine for a
//! `VirtualizedFileDiff` under a `Virtualizer` and renders a scroller around the diff; the
//! events of its nodes reach the managers through `events.rs`. Edits the managers make from
//! those events reach the document the same way as any other later edit.

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use dioxus::prelude::*;
use pierre_diffs::components::file_diff::DefaultFileDiffHooks;
use pierre_diffs::components::file_diff::FileDiff;
use pierre_diffs::components::file_diff::FileDiffOptions;
use pierre_diffs::components::file_diff::FileDiffRenderProps;
use pierre_diffs::components::virtualized_file_diff::instance::VirtualizedFileDiff;
use pierre_diffs::components::virtualizer::InstanceRef;
use pierre_diffs::components::virtualizer::Root;
use pierre_diffs::components::virtualizer::Virtualizer;
use pierre_diffs::components::virtualizer::VirtualizerConfig;
use pierre_diffs::highlighter_types::DiffsHighlighter;
use pierre_diffs::highlighter_types::PlainTextHighlighter;
use pierre_diffs::js_collections::PtrEq;
use pierre_diffs::managers::interaction_manager::InteractionManager;
use pierre_diffs::managers::interaction_manager::LineHoverHighlight;
use pierre_diffs::managers::resize_manager::ResizeObserverHub;
use pierre_diffs::managers::universal_rendering_manager::UniversalRenderingManager;
use pierre_diffs::types::options::DiffStyle;
use pierre_diffs::types::patch::FileContents;
use pierre_diffs::types::patch::FileDiffMetadata;
use pierre_diffs::types::patch::ParsePatchOptions;
use pierre_diffs::types::view::DiffLineAnnotation;
use pierre_diffs::types::view::SelectedLineRange;
use pierre_diffs::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use pierre_diffs::utils::compute_virtual_file_metrics::compute_virtual_file_metrics;
use pierre_diffs::utils::parse_patch_files::ParsePatchError;
use pierre_diffs::utils::parse_patch_files::parse_patch_files;

use crate::events::Dispatch;
use crate::events::ObservingManagers;
use crate::events::Pointers;
use crate::host::DioxusHost;
use crate::host::SleepFuture;
use crate::render::Renderer;
use crate::slot::SlotContent;
use crate::slot::SlotTable;

/// What to diff.
#[derive(Clone)]
pub enum DiffInput {
    /// Two versions of a file; the port computes the diff.
    Files {
        old_file: Box<FileContents>,
        new_file: Box<FileContents>,
    },
    /// An already parsed diff (see [`DiffInput::from_patch`]). Compared by identity, as the port
    /// compares diff targets.
    Parsed(Rc<FileDiffMetadata>),
}

impl DiffInput {
    /// The first file of a unified or git patch. The patch comes from outside the program, so a
    /// malformed one is returned for the caller to handle at its edge.
    pub fn from_patch(patch: &str) -> Result<Self, ParsePatchError> {
        let options = ParsePatchOptions {
            throw_on_error: Some(true),
            ..ParsePatchOptions::default()
        };
        let parsed = parse_patch_files(patch, &options)?;
        let first = parsed
            .value
            .into_iter()
            .flat_map(|patch| patch.files)
            .next()
            .ok_or_else(|| ParsePatchError {
                message: "patch contains no file diff".to_owned(),
            })?;
        Ok(Self::Parsed(Rc::new(first)))
    }
}

impl PartialEq for DiffInput {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Files {
                    old_file: old_a,
                    new_file: new_a,
                },
                Self::Files {
                    old_file: old_b,
                    new_file: new_b,
                },
            ) => old_a == old_b && new_a == new_b,
            (Self::Parsed(a), Self::Parsed(b)) => Rc::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// Virtualizes the diff: only the rows near the viewport are in the DOM, inside a scroller the
/// view renders. The scroller's height is the embedder's, because it is the one thing a
/// virtualizer cannot derive: a scroller that grows with its content has nothing to virtualize.
#[derive(Clone, PartialEq)]
pub struct Virtualization {
    /// The scroller's CSS `height` (`"40rem"`, `"100%"` of a bounded parent).
    pub height: String,
    /// Timers: a future that resolves after the duration. Standard Dioxus has no clock, so the
    /// embedder supplies the platform's (`tokio::time::sleep` on desktop, the browser's timer on
    /// the web). It is read when the view mounts.
    pub sleep: PtrEq<dyn Fn(Duration) -> SleepFuture>,
}

/// What the pointer does in the view: Pierre's interaction options, minus the ones that hand out
/// the view's internal nodes. Off by default, as upstream's are.
#[derive(Clone, PartialEq, Default)]
pub struct Interaction {
    /// Which part of the line under the mouse is highlighted (`data-hovered`).
    pub line_hover_highlight: LineHoverHighlight,
    /// Clicking a line number selects the line, shift-click extends, dragging selects a range.
    pub enable_line_selection: bool,
    /// The built-in button that follows the hovered line or the selection.
    pub enable_gutter_utility: bool,
    /// The selection after a click or a drag, `None` when it was cleared.
    pub on_line_selected: Option<Callback<Option<SelectedLineRange>>>,
    /// The built-in gutter utility's click: a drag from the button selects a range and ends in
    /// this call with it. Giving it selects that gesture's behaviour, so it needs
    /// `enable_gutter_utility`.
    pub on_gutter_utility_click: Option<Callback<SelectedLineRange>>,
}

#[derive(Props, Clone, PartialEq)]
pub struct FileDiffViewProps<A: Clone + PartialEq + 'static> {
    pub input: DiffInput,
    /// Unified or split; Pierre's default is split.
    #[props(default = DiffStyle::Split)]
    pub diff_style: DiffStyle,
    /// Show the unchanged lines between hunks instead of collapsing them behind separators.
    #[props(default)]
    pub expand_unchanged: bool,
    /// Line annotations: a comment thread, a lint note, an embedded micro. Each is placed under
    /// its line through `render_annotation`.
    #[props(default)]
    pub annotations: Vec<DiffLineAnnotation<A>>,
    /// Content for an annotation. `FileDiff` asks once per annotation and keeps the placement;
    /// the callback itself runs on every render of this component, so it may read signals.
    #[props(default)]
    pub render_annotation: Option<Callback<DiffLineAnnotation<A>, Element>>,
    /// The tokenizer behind syntax highlighting; plain text when absent. Read when the view
    /// mounts: a different highlighter needs a new view.
    #[props(default)]
    pub highlighter: Option<PtrEq<dyn DiffsHighlighter>>,
    /// Render only the rows near the viewport. This is a mode of the one component (not a second
    /// component) because everything else it takes is the same; like the highlighter it is read
    /// when the view mounts, and switching it needs a new view.
    #[props(default)]
    pub virtualized: Option<Virtualization>,
    /// Hover, selection and the gutter utility; none by default. Hunk expansion needs no option:
    /// a collapsed hunk's separator expands on click.
    #[props(default)]
    pub interaction: Interaction,
}

type AnnotationCallback<A> = Rc<RefCell<Option<Callback<DiffLineAnnotation<A>, Element>>>>;

/// The caller's current interaction callbacks, read when the manager calls them.
type InteractionCallbacks = Rc<RefCell<Interaction>>;

/// The props that become `FileDiffOptions`; a change needs `set_options` and a forced render.
#[derive(Clone, Copy, PartialEq)]
struct ViewOptions {
    diff_style: DiffStyle,
    expand_unchanged: bool,
    line_hover_highlight: LineHoverHighlight,
    enable_line_selection: bool,
    enable_gutter_utility: bool,
    /// Whether `on_gutter_utility_click` is given: the manager reads its presence.
    has_gutter_utility_click: bool,
}

impl ViewOptions {
    fn of<A: Clone + PartialEq + 'static>(props: &FileDiffViewProps<A>) -> Self {
        Self {
            diff_style: props.diff_style,
            expand_unchanged: props.expand_unchanged,
            line_hover_highlight: props.interaction.line_hover_highlight,
            enable_line_selection: props.interaction.enable_line_selection,
            enable_gutter_utility: props.interaction.enable_gutter_utility,
            has_gutter_utility_click: props.interaction.on_gutter_utility_click.is_some(),
        }
    }
}

struct Applied<A> {
    input: DiffInput,
    annotations: Rc<Vec<DiffLineAnnotation<A>>>,
    options: ViewOptions,
}

/// What renders into the document: the plain `FileDiff`, or the `VirtualizedFileDiff` that
/// extends it.
enum Engine<A: Clone + PartialEq + 'static> {
    Static(Rc<FileDiff<DioxusHost, SlotContent, A>>),
    Virtualized {
        instance: Rc<VirtualizedFileDiff<DioxusHost, SlotContent, A>>,
        virtualizer: Rc<Virtualizer<DioxusHost>>,
    },
}

impl<A: Clone + PartialEq + 'static> Engine<A> {
    fn set_options(
        &self,
        options: FileDiffOptions<pierre_diffs::document::NodeKey, SlotContent, A>,
    ) {
        match self {
            Self::Static(file_diff) => file_diff.set_options(Some(options)),
            Self::Virtualized { instance, .. } => instance.set_options(Some(options)),
        }
    }

    fn render(&self, props: FileDiffRenderProps<A>) {
        match self {
            Self::Static(file_diff) => {
                file_diff.lifecycle().render(props);
            }
            Self::Virtualized { instance, .. } => {
                instance.render(props);
            }
        }
    }

    fn clean_up(&self) {
        match self {
            Self::Static(file_diff) => file_diff.lifecycle().clean_up(false),
            Self::Virtualized {
                instance,
                virtualizer,
            } => {
                instance.clean_up(false);
                virtualizer.clean_up();
            }
        }
    }
}

struct View<A: Clone + PartialEq + 'static> {
    host: Rc<DioxusHost>,
    engine: Engine<A>,
    /// The listeners' destination.
    dispatch: Rc<Dispatch>,
    /// Where the pointer events of the interaction handlers go.
    pointers: Rc<Pointers>,
    interaction: InteractionCallbacks,
    slots: Rc<SlotTable>,
    /// The caller's current callback, read when an annotation's content is made.
    render_annotation: AnnotationCallback<A>,
    /// True while `sync` runs; see the module docs.
    syncing: Rc<Cell<bool>>,
    applied: RefCell<Option<Applied<A>>>,
}

fn file_diff_options<A: Clone + 'static>(
    view_options: ViewOptions,
    slots: &Rc<SlotTable>,
    render_annotation: &AnnotationCallback<A>,
    interaction: &InteractionCallbacks,
) -> FileDiffOptions<pierre_diffs::document::NodeKey, SlotContent, A> {
    let mut options = FileDiffOptions::default();
    options.base.diff_style = Some(view_options.diff_style);
    options.base.expand_unchanged = Some(view_options.expand_unchanged);
    options.interaction.line_hover_highlight = Some(view_options.line_hover_highlight);
    options.interaction.enable_line_selection = Some(view_options.enable_line_selection);
    options.interaction.enable_gutter_utility = Some(view_options.enable_gutter_utility);
    // The callbacks read the caller's current ones when they run (`Callback` is `Copy`, so the
    // borrow ends before the call, which may re-enter `sync`).
    let callbacks = Rc::clone(interaction);
    options.interaction.on_line_selected = Some(Rc::new(move |range| {
        let callback = callbacks.borrow().on_line_selected;
        if let Some(callback) = callback {
            callback.call(range.cloned());
        }
    }));
    if view_options.has_gutter_utility_click {
        let callbacks = Rc::clone(interaction);
        options.interaction.on_gutter_utility_click = Some(Rc::new(move |range| {
            let callback = callbacks.borrow().on_gutter_utility_click;
            callback
                .expect("the option is set only while the caller gives the callback")
                .call(range);
        }));
    }
    let slots = Rc::clone(slots);
    let render_annotation = Rc::clone(render_annotation);
    options.render_annotation = Some(PtrEq::new(Rc::new(move |annotation, _file_diff| {
        let callback = (*render_annotation.borrow())?;
        let annotation = annotation.clone();
        Some(SlotContent::new(&slots, move || {
            callback.call(annotation.clone())
        }))
    })));
    options
}

impl<A: Clone + PartialEq + 'static> View<A> {
    fn new(props: &FileDiffViewProps<A>, version: Signal<u64>) -> Self {
        let slots = Rc::new(SlotTable::default());
        let render_annotation: AnnotationCallback<A> = Rc::default();
        let interaction: InteractionCallbacks = Rc::new(RefCell::new(props.interaction.clone()));
        let highlighter: Rc<dyn DiffsHighlighter> = match &props.highlighter {
            Some(highlighter) => Rc::clone(&highlighter.0),
            None => Rc::new(PlainTextHighlighter),
        };
        let options = file_diff_options(
            ViewOptions::of(props),
            &slots,
            &render_annotation,
            &interaction,
        );
        let (host, engine, managers) = match &props.virtualized {
            None => {
                let host = DioxusHost::new_static();
                let file_diff = FileDiff::new(
                    Rc::clone(&host),
                    ResizeObserverHub::new(Rc::clone(&host)),
                    UniversalRenderingManager::new(Rc::clone(&host)),
                    highlighter,
                    Rc::new(DefaultFileDiffHooks),
                    options,
                    None,
                );
                (host, Engine::Static(file_diff), None)
            }
            Some(virtualization) => Self::virtualized(virtualization, highlighter, options),
        };
        let manager: Rc<InteractionManager<DioxusHost>> = match &engine {
            Engine::Static(file_diff) => Rc::clone(file_diff.interaction_manager()),
            Engine::Virtualized { instance, .. } => {
                Rc::clone(instance.file_diff().interaction_manager())
            }
        };
        let pointers = Pointers::new(Rc::clone(&host), manager);
        let syncing = Rc::new(Cell::new(false));
        let observer_syncing = Rc::clone(&syncing);
        host_document(&host).set_observer(Some(Rc::new(move |_document, _mutation| {
            if !observer_syncing.get() {
                // A signal is `Copy`; the write needs a binding of its own per call.
                let mut version = version;
                version += 1;
            }
        })));
        let dispatch = Rc::new(Dispatch {
            host: Rc::clone(&host),
            managers,
        });
        Self {
            host,
            engine,
            dispatch,
            pointers,
            interaction,
            slots,
            render_annotation,
            syncing,
            applied: RefCell::new(None),
        }
    }

    /// The pieces of a virtualized view: a scrolling host, the `Virtualizer` set up over its
    /// scroller, and the instance the virtualizer drives. Upstream's owner (the React component)
    /// calls `setup` when it mounts the scroller; the scroller's nodes exist in the document from
    /// the host's construction, so it is the same moment.
    fn virtualized(
        virtualization: &Virtualization,
        highlighter: Rc<dyn DiffsHighlighter>,
        options: FileDiffOptions<pierre_diffs::document::NodeKey, SlotContent, A>,
    ) -> (Rc<DioxusHost>, Engine<A>, Option<ObservingManagers>) {
        let metrics = PartialVirtualFileMetrics::default();
        let host = DioxusHost::new_scrolling(
            &virtualization.height,
            Rc::clone(&virtualization.sleep.0),
            compute_virtual_file_metrics(metrics).line_height,
        );
        let scroller = host.scroller().expect("a scrolling host has a scroller");
        let (root, content) = (scroller.root, scroller.content);
        let hub = ResizeObserverHub::new(Rc::clone(&host));
        let queue = UniversalRenderingManager::new(Rc::clone(&host));
        let virtualizer = Virtualizer::new(
            Rc::clone(&host),
            queue.clone(),
            VirtualizerConfig::default(),
        );
        virtualizer.setup(Root::Element(root), Some(content));
        let instance = VirtualizedFileDiff::new(
            Rc::clone(&host),
            Rc::clone(&hub),
            queue,
            highlighter,
            options,
            Rc::clone(&virtualizer),
            metrics,
            None,
        );
        let managers = ObservingManagers {
            hub,
            virtualizer: Rc::clone(&virtualizer),
            scroll_sync: Rc::clone(instance.file_diff().scroll_sync_manager()),
            instance: Rc::clone(&instance) as InstanceRef,
        };
        (
            host,
            Engine::Virtualized {
                instance,
                virtualizer,
            },
            Some(managers),
        )
    }

    /// Brings the `FileDiff` to `props`. Called from the component body, so the document is
    /// current when the same render reads it.
    fn sync(&self, props: &FileDiffViewProps<A>) {
        *self.render_annotation.borrow_mut() = props.render_annotation;
        *self.interaction.borrow_mut() = props.interaction.clone();
        self.host.pointer.set_rows(
            props.interaction.line_hover_highlight != LineHoverHighlight::Disabled
                || props.interaction.enable_line_selection
                || props.interaction.enable_gutter_utility
                || props.interaction.on_gutter_utility_click.is_some(),
        );
        let unchanged = self.applied.borrow().as_ref().is_some_and(|applied| {
            applied.input == props.input
                && applied.options == ViewOptions::of(props)
                && *applied.annotations == props.annotations
        });
        if unchanged {
            return;
        }
        let annotations = Rc::new(props.annotations.clone());
        let options_changed = self
            .applied
            .borrow()
            .as_ref()
            .is_some_and(|applied| applied.options != ViewOptions::of(props));
        self.syncing.set(true);
        if options_changed {
            self.engine.set_options(file_diff_options(
                ViewOptions::of(props),
                &self.slots,
                &self.render_annotation,
                &self.interaction,
            ));
        }
        let mut render_props = FileDiffRenderProps {
            file_container: Some(self.host.container()),
            line_annotations: Some(Rc::clone(&annotations)),
            force_render: options_changed,
            ..FileDiffRenderProps::default()
        };
        match &props.input {
            DiffInput::Files { old_file, new_file } => {
                render_props.old_file = Some(Some((**old_file).clone()));
                render_props.new_file = Some(Some((**new_file).clone()));
            }
            DiffInput::Parsed(file_diff) => render_props.file_diff = Some(Rc::clone(file_diff)),
        }
        self.engine.render(render_props);
        self.syncing.set(false);
        *self.applied.borrow_mut() = Some(Applied {
            input: props.input.clone(),
            annotations,
            options: ViewOptions::of(props),
        });
    }

    fn element(&self) -> Element {
        let document = host_document(&self.host);
        self.slots.prune(document);
        self.host.prune();
        Renderer {
            document,
            slots: &self.slots,
            dispatch: &self.dispatch,
            pointers: &self.pointers,
        }
        .render(self.host.top())
    }

    fn clean_up(&self) {
        host_document(&self.host).set_observer(None);
        self.engine.clean_up();
    }
}

fn host_document(host: &DioxusHost) -> &pierre_diffs::document::Document {
    pierre_diffs::managers::resize_manager::ResizeManagerHost::document(host)
}

/// The view's state for this component instance, brought up to `props`.
fn use_view<A: Clone + PartialEq + 'static>(props: &FileDiffViewProps<A>) -> Rc<View<A>> {
    let version = use_signal(|| 0_u64);
    let view = use_hook(|| Rc::new(View::new(props, version)));
    view.sync(props);
    // Subscribes this render to edits made outside `sync`.
    let _: u64 = version();
    use_drop({
        let view = Rc::clone(&view);
        move || view.clean_up()
    });
    view
}

#[component]
pub fn FileDiffView<A: Clone + PartialEq + 'static>(props: FileDiffViewProps<A>) -> Element {
    use_view(&props).element()
}

#[cfg(test)]
mod interaction_tests;
#[cfg(test)]
mod virtual_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use pierre_diffs::types::view::AnnotationSide;

    fn file(name: &str, contents: &str) -> Box<FileContents> {
        Box::new(FileContents {
            name: name.to_owned(),
            contents: contents.to_owned(),
            lang: None,
            header: None,
            cache_key: None,
        })
    }

    fn input() -> DiffInput {
        DiffInput::Files {
            old_file: file(
                "a.rs",
                "fn main() {\n    let x = 1;\n    println!(\"{x}\");\n}\n",
            ),
            new_file: file(
                "a.rs",
                "fn main() {\n    let x = 2;\n    println!(\"{x}\");\n}\n",
            ),
        }
    }

    /// The style lives in a signal the test can set, to drive a re-render from outside.
    type StyleSignal = Rc<Cell<Option<Signal<DiffStyle>>>>;

    #[component]
    fn Sample(style: StyleSignal) -> Element {
        let diff_style = use_signal(|| DiffStyle::Split);
        style.set(Some(diff_style));
        rsx! {
            FileDiffView::<&'static str> {
                input: input(),
                diff_style: diff_style(),
                annotations: vec![DiffLineAnnotation {
                    side: AnnotationSide::Additions,
                    line_number: 2,
                    metadata: Some("looks right"),
                }],
                render_annotation: Callback::new(|annotation: DiffLineAnnotation<&'static str>| {
                    rsx! {
                        aside { class: "note", "annotation: {annotation.metadata:?}" }
                    }
                }),
            }
        }
    }

    /// The page with the two bulky constants elided: the 30 KB stylesheet (its presence and
    /// position are what matter) and the icon sprite.
    fn outline(dom: &VirtualDom) -> String {
        let html = dioxus_ssr::render(dom);
        let (style_start, rest) = html
            .split_once("<style>")
            .expect("the shadow host starts with the core stylesheet");
        let (css, rest) = rest.split_once("</style>").expect("the style closes");
        assert!(
            css.starts_with("@layer base, theme, rendered, unsafe;"),
            "the first style is the core CSS, got {:?}",
            &css[..css.len().min(60)]
        );
        let sprite_start = rest.find("<svg aria-hidden").expect("the icon sprite");
        let sprite_end = rest[sprite_start..].find("</svg>").expect("sprite closes") + sprite_start;
        let (before, after) = (&rest[..sprite_start], &rest[sprite_end + "</svg>".len()..]);
        // The theme style carries generated CSS of the same bulk.
        let (theme_head, theme_tail) = after
            .split_once("<style data-theme-css=\"\">")
            .expect("the theme style");
        let (_, theme_tail) = theme_tail.split_once("</style>").expect("the theme closes");
        format!(
            "{style_start}<style><core css></style>{before}<svg sprite></svg>{theme_head}\
             <style data-theme-css=\"\"><theme css></style>{theme_tail}"
        )
        .replace("><", ">\n<")
    }

    fn mounted() -> (VirtualDom, StyleSignal) {
        let style = StyleSignal::default();
        let mut dom = VirtualDom::new_with_props(
            Sample,
            SampleProps {
                style: Rc::clone(&style),
            },
        );
        dom.rebuild_in_place();
        (dom, style)
    }

    #[test]
    fn split_diff_renders_shadow_host_core_css_and_annotation_slot() {
        let (dom, _) = mounted();
        insta::assert_snapshot!(outline(&dom));
    }

    // The host's methods a static render must not reach are `todo!`, and Dioxus swallows a
    // panicking render into an empty page, which `outline` rejects (no shadow host to find).
    #[test]
    fn re_rendering_with_a_new_style_replaces_the_rows() {
        let (mut dom, style) = mounted();
        let split = outline(&dom);
        assert!(split.contains("data-diff-type=\"split\""));
        let mut style = style.get().expect("the sample stored its signal");
        dom.in_runtime(|| style.set(DiffStyle::Unified));
        dom.render_immediate_to_vec();
        let unified = outline(&dom);
        // Pierre calls the unified layout `single` and marks its one code column `data-unified`.
        assert!(unified.contains("data-diff-type=\"single\"") && unified.contains("data-unified"));
        assert!(
            !unified.contains("data-diff-type=\"split\"") && !unified.contains("data-deletions=")
        );
        // The annotation slot survives the switch: its callback is asked again.
        assert!(unified.contains("annotation: Some(&#34;looks right&#34;)"));
    }

    #[test]
    fn dropping_the_view_cleans_the_diff_up() {
        let (dom, _) = mounted();
        // `FileDiff::clean_up` calls the host's `clear_timeout(-1)`, among others.
        drop(dom);
    }

    #[component]
    fn FromPatch() -> Element {
        let input =
            DiffInput::from_patch("--- a.txt\n+++ a.txt\n@@ -1,2 +1,2 @@\n one\n-two\n+three\n")
                .expect("a well-formed patch");
        rsx! {
            FileDiffView::<()> { input }
        }
    }

    #[test]
    fn a_parsed_patch_renders_without_annotations() {
        let mut dom = VirtualDom::new(FromPatch);
        dom.rebuild_in_place();
        let page = outline(&dom);
        assert!(
            page.contains("<bdi>a.txt</bdi>")
                && page.contains("data-line-type=\"change-addition\"")
        );
        assert!(!page.contains("data-annotation-slot"));
    }

    #[test]
    fn a_malformed_patch_is_an_error_for_the_caller() {
        assert!(DiffInput::from_patch("not a patch").is_err());
    }
}
