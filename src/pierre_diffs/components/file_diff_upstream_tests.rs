//! Upstream's `FileDiff`-level tests that `render` makes satisfiable, ported
//! with their names (`test/FileDiff.headerSlots.test.ts`,
//! `FileDiff.rerenderInputs.test.ts`, `FileDiff.partialRender.test.ts`), over
//! `FakeDom` and the plain-text stub highlighter. The waits for async
//! highlighting are gone (rendering is synchronous), `instance.cleanUp()` waits
//! for 2c, and the tests that need the edit session are not ported (see
//! parity/divergences.psv). Test-only.

use std::rc::Rc;

use crate::components::file_diff::FileDiff;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::FileDiffRenderProps;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::hast;
use crate::highlighter_types::PlainTextHighlighter;
use crate::js_collections::PtrEq;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::layout::RenderRange;
use crate::types::options::DiffStyle;
use crate::types::patch::ChangeTypes;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;
use crate::utils::parse_diff_from_file::parse_diff_from_file;

type Instance = FileDiff<FakeDom, hast::Element, String>;
type Options = FileDiffOptions<NodeKey, hast::Element, String>;

fn file(name: &str, contents: &str) -> FileContents {
    FileContents {
        name: name.to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    }
}

fn parse(old_file: FileContents, new_file: FileContents) -> Rc<FileDiffMetadata> {
    Rc::new(
        parse_diff_from_file(
            &DiffFileInput::Both { old_file, new_file },
            &Default::default(),
            false,
        )
        .expect("the files diff")
        .value,
    )
}

fn instance(dom: &Rc<FakeDom>, options: Options) -> Rc<Instance> {
    Instance::new(
        Rc::clone(dom),
        ResizeObserverHub::new(Rc::clone(dom)),
        UniversalRenderingManager::new(Rc::clone(dom)),
        Rc::new(PlainTextHighlighter),
        options,
        None,
    )
}

fn window(starting_line: f64, total_lines: f64) -> Option<RenderRange> {
    Some(RenderRange {
        starting_line,
        total_lines,
        buffer_before: 0.0,
        buffer_after: 0.0,
    })
}

fn slot_content(text: &str) -> hast::Element {
    hast::Element::new(
        "span",
        hast::Properties::new(),
        vec![hast::Text::new(text).into()],
    )
}

/// `container.querySelector('[slot="..."]')?.textContent`
fn slot_text(dom: &FakeDom, container: NodeKey, slot: &str) -> Option<String> {
    dom.doc
        .query_by_attribute(container, "slot", Some(slot))
        .first()
        .map(|element| dom.doc.text_content(*element))
}

fn shadow_root(dom: &FakeDom, container: NodeKey) -> NodeKey {
    dom.doc.shadow_root(container).expect("a shadow root")
}

fn first_line(dom: &FakeDom, container: NodeKey) -> Option<NodeKey> {
    dom.doc
        .query_by_attribute(shadow_root(dom, container), "data-line", Some("1"))
        .first()
        .copied()
}

#[test]
fn header_slots_renders_updates_and_removes_the_filename_suffix_slot() {
    let dom = Rc::new(FakeDom::default());
    let file_diff = parse(
        file("example.txt", "value 1\n"),
        file("example.txt", "value 2\n"),
    );
    let file_container = dom.doc.create_element("div");
    let suffix = |text: &'static str| {
        Some(PtrEq(
            Rc::new(move |_: &FileDiffMetadata| Some(slot_content(text))) as Rc<_>,
        ))
    };
    let mut options = Options::default();
    options.base.base.collapsed = Some(true);
    options.render_header_filename_suffix = suffix("initial suffix");
    let instance = instance(&dom, options);
    let render = |force_render: bool| {
        instance.render(FileDiffRenderProps {
            file_diff: Some(Rc::clone(&file_diff)),
            file_container: Some(file_container),
            prevent_emit: true,
            force_render,
            ..FileDiffRenderProps::default()
        });
    };

    render(false);
    assert_eq!(
        slot_text(&dom, file_container, "header-filename-suffix").as_deref(),
        Some("initial suffix")
    );

    let mut options = instance.options().clone();
    options.render_header_filename_suffix = suffix("updated suffix");
    instance.set_options(Some(options));
    render(true);
    assert_eq!(
        slot_text(&dom, file_container, "header-filename-suffix").as_deref(),
        Some("updated suffix")
    );

    let mut options = instance.options().clone();
    options.render_header_filename_suffix = Some(PtrEq(Rc::new(|_: &FileDiffMetadata| None)));
    instance.set_options(Some(options));
    render(true);
    assert_eq!(
        slot_text(&dom, file_container, "header-filename-suffix"),
        None
    );
}

#[test]
fn header_slots_removes_the_filename_suffix_slot_when_a_custom_header_is_active() {
    let dom = Rc::new(FakeDom::default());
    let file_diff = parse(
        file("example.txt", "value 1\n"),
        file("example.txt", "value 2\n"),
    );
    let file_container = dom.doc.create_element("div");
    let mut options = Options::default();
    options.base.base.collapsed = Some(true);
    options.render_header_filename_suffix = Some(PtrEq(Rc::new(|_: &FileDiffMetadata| {
        Some(slot_content("suffix"))
    })));
    let instance = instance(&dom, options);
    let render = |force_render: bool| {
        instance.render(FileDiffRenderProps {
            file_diff: Some(Rc::clone(&file_diff)),
            file_container: Some(file_container),
            prevent_emit: true,
            force_render,
            ..FileDiffRenderProps::default()
        });
    };

    render(false);
    assert_eq!(
        slot_text(&dom, file_container, "header-filename-suffix").as_deref(),
        Some("suffix")
    );

    let mut options = instance.options().clone();
    options.render_custom_header = Some(PtrEq(Rc::new(|_: &FileDiffMetadata| {
        Some(slot_content("custom header"))
    })));
    instance.set_options(Some(options));
    render(true);
    assert_eq!(
        slot_text(&dom, file_container, "header-filename-suffix"),
        None
    );
    assert_eq!(
        slot_text(&dom, file_container, "header-custom").as_deref(),
        Some("custom header")
    );
}

#[test]
fn rerender_inputs_a_host_render_after_an_internal_rerender_takes_the_early_return_path() {
    let dom = Rc::new(FakeDom::default());
    let old_file = file("x.ts", "alpha\nbravo\ncharlie\n");
    let new_file = file("x.ts", "alpha\nBRAVO\ncharlie\n");
    let file_container = dom.doc.create_element("div");
    let mut options = Options::default();
    options.base.base.disable_file_header = Some(true);
    options.base.diff_style = Some(DiffStyle::Split);
    let instance = instance(&dom, options);
    let render = || {
        instance.render(FileDiffRenderProps {
            old_file: Some(Some(old_file.clone())),
            new_file: Some(Some(new_file.clone())),
            file_container: Some(file_container),
            ..FileDiffRenderProps::default()
        })
    };

    assert!(render());

    // An internal input-less rerender: what every hunk expansion triggers.
    instance.rerender();
    let row_before = first_line(&dom, file_container);
    assert!(row_before.is_some());

    // Host render re-passing the same files (the documented oldFile/newFile API
    // shape): same inputs, no force, no theme or annotation change. It must
    // early-return and leave the DOM untouched.
    assert!(!render());
    assert_eq!(first_line(&dom, file_container), row_before);
}

#[test]
fn rerender_inputs_parsed_unkeyed_diffs_with_the_same_filename_render_fresh_contents() {
    let dom = Rc::new(FakeDom::default());
    let first_diff = parse(
        file("same.ts", "const base = 0;\n"),
        file("same.ts", "const firstMarker = 1;\n"),
    );
    let second_diff = parse(
        file("same.ts", "const base = 0;\n"),
        file("same.ts", "const secondMarker = 2;\n"),
    );
    let file_container = dom.doc.create_element("div");
    let mut options = Options::default();
    options.base.base.disable_file_header = Some(true);
    options.base.diff_style = Some(DiffStyle::Unified);
    let instance = instance(&dom, options);

    assert!(first_diff.cache_key.is_none());
    assert!(second_diff.cache_key.is_none());

    let text = || dom.doc.text_content(shadow_root(&dom, file_container));
    instance.render(FileDiffRenderProps {
        file_diff: Some(Rc::clone(&first_diff)),
        file_container: Some(file_container),
        ..FileDiffRenderProps::default()
    });
    assert!(text().contains("firstMarker"));

    instance.render(FileDiffRenderProps {
        file_diff: Some(Rc::clone(&second_diff)),
        file_container: Some(file_container),
        ..FileDiffRenderProps::default()
    });
    assert!(text().contains("secondMarker"));
    assert!(!text().contains("firstMarker"));
    assert!(first_diff.cache_key.is_none());
    assert!(second_diff.cache_key.is_none());
}

fn partial_render_instance(dom: &Rc<FakeDom>) -> Rc<Instance> {
    let mut options = Options::default();
    options.base.base.disable_file_header = Some(true);
    options.base.diff_style = Some(DiffStyle::Split);
    instance(dom, options)
}

fn annotated_render(
    instance: &Instance,
    file_diff: &Rc<FileDiffMetadata>,
    file_container: NodeKey,
    annotations: &Rc<Vec<DiffLineAnnotation<String>>>,
    range: Option<RenderRange>,
) {
    instance.render(FileDiffRenderProps {
        file_container: Some(file_container),
        file_diff: Some(Rc::clone(file_diff)),
        line_annotations: Some(Rc::clone(annotations)),
        defer_managers: true,
        prevent_emit: true,
        render_range: range,
        ..FileDiffRenderProps::default()
    });
}

// Crash regression guard: re-rendering a narrower range over an annotated
// deleted line exercises the partial-render trim path. Programming errors
// panic here, so completing the render is the assertion; the resulting DOM is
// not inspected, so column alignment itself is not verified.
#[test]
fn partial_render_re_rendering_a_narrower_range_over_an_annotated_deleted_line_does_not_throw() {
    let dom = Rc::new(FakeDom::default());
    let file_diff = parse(file("x.txt", "a\nb\nc\nd\n"), file("x.txt", "a\nd\n"));
    let file_container = dom.doc.create_element("div");
    let annotations = Rc::new(vec![DiffLineAnnotation {
        side: AnnotationSide::Deletions,
        line_number: 2,
        metadata: Some("annotation".to_owned()),
    }]);
    let instance = partial_render_instance(&dom);

    annotated_render(
        &instance,
        &file_diff,
        file_container,
        &annotations,
        window(1.0, 3.0),
    );
    assert!(
        !dom.doc
            .query_by_attribute(shadow_root(&dom, file_container), "data-code", None)
            .is_empty()
    );

    annotated_render(
        &instance,
        &file_diff,
        file_container,
        &annotations,
        window(2.0, 2.0),
    );
}

#[test]
fn partial_render_re_rendering_a_range_that_excludes_the_top_removes_file_level_annotations() {
    let dom = Rc::new(FakeDom::default());
    let file_diff = parse(file("x.txt", "a\nb\nc\nd\n"), file("x.txt", "a\nB\nc\nd\n"));
    let file_container = dom.doc.create_element("div");
    let annotations = Rc::new(vec![
        DiffLineAnnotation {
            side: AnnotationSide::Deletions,
            line_number: 0,
            metadata: Some("old-file".to_owned()),
        },
        DiffLineAnnotation {
            side: AnnotationSide::Additions,
            line_number: 0,
            metadata: Some("new-file".to_owned()),
        },
    ]);
    let instance = partial_render_instance(&dom);
    let file_level_annotations = || {
        dom.doc
            .query_by_attribute(
                shadow_root(&dom, file_container),
                "data-line-annotation",
                Some("-1,-1"),
            )
            .len()
    };

    annotated_render(
        &instance,
        &file_diff,
        file_container,
        &annotations,
        window(0.0, 2.0),
    );
    assert_eq!(file_level_annotations(), 2);

    annotated_render(
        &instance,
        &file_diff,
        file_container,
        &annotations,
        window(1.0, 2.0),
    );
    assert_eq!(file_level_annotations(), 0);
}

#[test]
fn partial_render_renders_a_new_file_from_an_explicit_missing_old_file_side() {
    let dom = Rc::new(FakeDom::default());
    let file_container = dom.doc.create_element("div");
    let instance = partial_render_instance(&dom);

    instance.render(FileDiffRenderProps {
        file_container: Some(file_container),
        old_file: Some(None),
        new_file: Some(Some(file("new.txt", "alpha\nbeta\n"))),
        defer_managers: true,
        prevent_emit: true,
        ..FileDiffRenderProps::default()
    });

    let file_diff = instance.get_input_diff().expect("a diff");
    assert_eq!(file_diff.change_type, ChangeTypes::New);
    assert!(!file_diff.is_partial);
    assert!(file_diff.deletion_lines.is_empty());
    assert_eq!(file_diff.addition_lines, ["alpha\n", "beta\n"]);
}

#[test]
fn partial_render_renders_a_deleted_file_from_an_explicit_missing_new_file_side() {
    let dom = Rc::new(FakeDom::default());
    let file_container = dom.doc.create_element("div");
    let instance = partial_render_instance(&dom);

    instance.render(FileDiffRenderProps {
        file_container: Some(file_container),
        old_file: Some(Some(file("deleted.txt", "alpha\nbeta\n"))),
        new_file: Some(None),
        defer_managers: true,
        prevent_emit: true,
        ..FileDiffRenderProps::default()
    });

    let file_diff = instance.get_input_diff().expect("a diff");
    assert_eq!(file_diff.change_type, ChangeTypes::Deleted);
    assert!(!file_diff.is_partial);
    assert_eq!(file_diff.deletion_lines, ["alpha\n", "beta\n"]);
    assert!(file_diff.addition_lines.is_empty());
}
