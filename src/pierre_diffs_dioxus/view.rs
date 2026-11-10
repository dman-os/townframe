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

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;

use dioxus::prelude::*;
use pierre_diffs::components::file_diff::DefaultFileDiffHooks;
use pierre_diffs::components::file_diff::FileDiff;
use pierre_diffs::components::file_diff::FileDiffOptions;
use pierre_diffs::components::file_diff::FileDiffRenderProps;
use pierre_diffs::highlighter_types::DiffsHighlighter;
use pierre_diffs::highlighter_types::PlainTextHighlighter;
use pierre_diffs::js_collections::PtrEq;
use pierre_diffs::managers::resize_manager::ResizeObserverHub;
use pierre_diffs::managers::universal_rendering_manager::UniversalRenderingManager;
use pierre_diffs::types::options::DiffStyle;
use pierre_diffs::types::patch::FileContents;
use pierre_diffs::types::patch::FileDiffMetadata;
use pierre_diffs::types::patch::ParsePatchOptions;
use pierre_diffs::types::view::DiffLineAnnotation;
use pierre_diffs::utils::parse_patch_files::ParsePatchError;
use pierre_diffs::utils::parse_patch_files::parse_patch_files;

use crate::host::DioxusHost;
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

#[derive(Props, Clone, PartialEq)]
pub struct FileDiffViewProps<A: Clone + PartialEq + 'static> {
    pub input: DiffInput,
    /// Unified or split; Pierre's default is split.
    #[props(default = DiffStyle::Split)]
    pub diff_style: DiffStyle,
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
}

type AnnotationCallback<A> = Rc<RefCell<Option<Callback<DiffLineAnnotation<A>, Element>>>>;

struct Applied<A> {
    input: DiffInput,
    annotations: Rc<Vec<DiffLineAnnotation<A>>>,
    diff_style: DiffStyle,
}

struct View<A: Clone + PartialEq + 'static> {
    host: Rc<DioxusHost>,
    file_diff: Rc<FileDiff<DioxusHost, SlotContent, A>>,
    slots: Rc<SlotTable>,
    /// The caller's current callback, read when an annotation's content is made.
    render_annotation: AnnotationCallback<A>,
    /// True while `sync` runs; see the module docs.
    syncing: Rc<Cell<bool>>,
    applied: RefCell<Option<Applied<A>>>,
}

fn file_diff_options<A: Clone + 'static>(
    diff_style: DiffStyle,
    slots: &Rc<SlotTable>,
    render_annotation: &AnnotationCallback<A>,
) -> FileDiffOptions<pierre_diffs::document::NodeKey, SlotContent, A> {
    let mut options = FileDiffOptions::default();
    options.base.diff_style = Some(diff_style);
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
        let host = Rc::new(DioxusHost::new());
        let slots = Rc::new(SlotTable::default());
        let render_annotation: AnnotationCallback<A> = Rc::default();
        let highlighter: Rc<dyn DiffsHighlighter> = match &props.highlighter {
            Some(highlighter) => Rc::clone(&highlighter.0),
            None => Rc::new(PlainTextHighlighter),
        };
        let file_diff = FileDiff::new(
            Rc::clone(&host),
            ResizeObserverHub::new(Rc::clone(&host)),
            UniversalRenderingManager::new(Rc::clone(&host)),
            highlighter,
            Rc::new(DefaultFileDiffHooks),
            file_diff_options(props.diff_style, &slots, &render_annotation),
            None,
        );
        let syncing = Rc::new(Cell::new(false));
        let observer_syncing = Rc::clone(&syncing);
        host_document(&host).set_observer(Some(Rc::new(move |_document, _mutation| {
            if !observer_syncing.get() {
                // A signal is `Copy`; the write needs a binding of its own per call.
                let mut version = version;
                version += 1;
            }
        })));
        Self {
            host,
            file_diff,
            slots,
            render_annotation,
            syncing,
            applied: RefCell::new(None),
        }
    }

    /// Brings the `FileDiff` to `props`. Called from the component body, so the document is
    /// current when the same render reads it.
    fn sync(&self, props: &FileDiffViewProps<A>) {
        *self.render_annotation.borrow_mut() = props.render_annotation;
        let unchanged = self.applied.borrow().as_ref().is_some_and(|applied| {
            applied.input == props.input
                && applied.diff_style == props.diff_style
                && *applied.annotations == props.annotations
        });
        if unchanged {
            return;
        }
        let annotations = Rc::new(props.annotations.clone());
        let style_changed = self
            .applied
            .borrow()
            .as_ref()
            .is_some_and(|applied| applied.diff_style != props.diff_style);
        self.syncing.set(true);
        if style_changed {
            self.file_diff.set_options(Some(file_diff_options(
                props.diff_style,
                &self.slots,
                &self.render_annotation,
            )));
        }
        let mut render_props = FileDiffRenderProps {
            file_container: Some(self.host.container()),
            line_annotations: Some(Rc::clone(&annotations)),
            force_render: style_changed,
            ..FileDiffRenderProps::default()
        };
        match &props.input {
            DiffInput::Files { old_file, new_file } => {
                render_props.old_file = Some(Some((**old_file).clone()));
                render_props.new_file = Some(Some((**new_file).clone()));
            }
            DiffInput::Parsed(file_diff) => render_props.file_diff = Some(Rc::clone(file_diff)),
        }
        self.file_diff.lifecycle().render(render_props);
        self.syncing.set(false);
        *self.applied.borrow_mut() = Some(Applied {
            input: props.input.clone(),
            annotations,
            diff_style: props.diff_style,
        });
    }

    fn element(&self) -> Element {
        let document = host_document(&self.host);
        self.slots.prune(document);
        Renderer {
            document,
            slots: &self.slots,
        }
        .render(self.host.container())
    }

    fn clean_up(&self) {
        host_document(&self.host).set_observer(None);
        self.file_diff.lifecycle().clean_up(false);
    }
}

fn host_document(host: &DioxusHost) -> &pierre_diffs::document::Document {
    pierre_diffs::managers::resize_manager::ResizeManagerHost::document(host)
}

#[component]
pub fn FileDiffView<A: Clone + PartialEq + 'static>(props: FileDiffViewProps<A>) -> Element {
    let version = use_signal(|| 0_u64);
    let view = use_hook(|| Rc::new(View::new(&props, version)));
    view.sync(&props);
    // Subscribes this render to edits made outside `sync`.
    let _: u64 = version();
    use_drop({
        let view = Rc::clone(&view);
        move || view.clean_up()
    });
    view.element()
}

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
