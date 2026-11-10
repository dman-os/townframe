//! The ownership rule at the `FileDiff` level (no upstream counterpart): a
//! render replaces rows, columns, headers and slotted nodes and `retire`s them,
//! the managers' setup runs, `flush_managers` discards them; so rendering,
//! re-rendering with new files and partial rendering forever must keep the
//! `Document` at the size of one render; and `cleanUp` frees every node the
//! instance created but the container the caller passed. Test-only, original
//! to this crate.

use std::rc::Rc;

use crate::components::file_diff::FileDiff;
use crate::components::file_diff::FileDiffHunkSeparators;
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
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;

type Instance = FileDiff<FakeDom, hast::Element, i64>;

fn file(contents: String) -> FileContents {
    FileContents {
        name: "a.txt".to_owned(),
        contents,
        lang: None,
        header: None,
        cache_key: None,
    }
}

/// Fifty numbered lines with the `edit`th line changed.
fn pair(edit: usize) -> (FileContents, FileContents) {
    let lines = |changed: Option<usize>| -> String {
        (0..50)
            .map(|index| {
                if Some(index) == changed {
                    format!("edited {index}\n")
                } else {
                    format!("line {index}\n")
                }
            })
            .collect()
    };
    (file(lines(None)), file(lines(Some(edit % 50))))
}

fn window(starting_line: usize, total_lines: usize) -> RenderRange {
    RenderRange {
        starting_line: starting_line as f64,
        total_lines: total_lines as f64,
        buffer_before: 0.0,
        buffer_after: 0.0,
    }
}

fn instance(
    dom: &Rc<FakeDom>,
    options: FileDiffOptions<NodeKey, hast::Element, i64>,
) -> Rc<Instance> {
    Instance::new(
        Rc::clone(dom),
        ResizeObserverHub::new(Rc::clone(dom)),
        UniversalRenderingManager::new(Rc::clone(dom)),
        Rc::new(PlainTextHighlighter),
        options,
        None,
    )
}

fn render_files(instance: &Instance, edit: usize, range: Option<RenderRange>) {
    let (old_file, new_file) = pair(edit);
    assert!(instance.lifecycle().render(FileDiffRenderProps {
        old_file: Some(Some(old_file)),
        new_file: Some(Some(new_file)),
        render_range: range,
        ..FileDiffRenderProps::default()
    }));
}

/// Runs `step` for `rounds` rounds and asserts the document stopped growing
/// after the first `warm_up`, and that no retired node outlives its render.
fn assert_bounded(dom: &Rc<FakeDom>, instance: &Instance, rounds: usize, step: impl Fn(usize)) {
    let warm_up = 40;
    let mut warm_capacity = 0;
    for round in 0..rounds {
        step(round);
        assert!(
            instance.state().retired.is_empty(),
            "round {round}: a render left retired nodes behind"
        );
        if round == warm_up {
            warm_capacity = dom.doc.capacity();
        }
    }
    assert!(
        dom.doc.capacity() <= warm_capacity + warm_capacity / 10,
        "the document grew from {warm_capacity} to {} slots",
        dom.doc.capacity()
    );
}

#[test]
fn re_rendering_with_new_files_keeps_the_document_bounded() {
    let dom = Rc::new(FakeDom::default());
    let instance = instance(&dom, FileDiffOptions::default());
    assert_bounded(&dom, &instance, 400, |round| {
        render_files(&instance, round, None);
    });
}

#[test]
fn partial_renders_keep_the_document_bounded() {
    let dom = Rc::new(FakeDom::default());
    let mut options = FileDiffOptions::default();
    options.base.expand_unchanged = Some(true);
    let instance = instance(&dom, options);
    // A window sliding down the file and back: overlapping windows trim and
    // append rows, a jump re-renders in full.
    assert_bounded(&dom, &instance, 600, |round| {
        let start = (round * 3) % 40;
        render_files(&instance, 7, Some(window(start, 12)));
    });
}

#[test]
fn switching_styles_and_collapsing_keeps_the_document_bounded() {
    let dom = Rc::new(FakeDom::default());
    let instance = instance(&dom, FileDiffOptions::default());
    assert_bounded(&dom, &instance, 400, |round| {
        let mut options = FileDiffOptions::default();
        options.base.diff_style = Some(if round % 2 == 0 {
            crate::types::options::DiffStyle::Split
        } else {
            crate::types::options::DiffStyle::Unified
        });
        options.base.base.collapsed = Some(round % 5 == 4);
        instance.set_options(Some(options));
        render_files(&instance, round, None);
    });
}

fn span(text: &str) -> hast::Element {
    hast::Element::new(
        "span",
        hast::Properties::new(),
        vec![hast::Text::new(text).into()],
    )
}

/// Every callback that makes the instance create nodes of its own: header
/// slots, annotation rows, custom separators and the gutter utility.
fn options_that_create_everything() -> FileDiffOptions<NodeKey, hast::Element, i64> {
    FileDiffOptions {
        render_header_prefix: Some(PtrEq(Rc::new(|_: &FileDiffMetadata| Some(span("p"))))),
        render_header_metadata: Some(PtrEq(Rc::new(|_: &FileDiffMetadata| Some(span("m"))))),
        render_annotation: Some(PtrEq(Rc::new(
            |_: &DiffLineAnnotation<i64>, _: &FileDiffMetadata| Some(span("note")),
        ))),
        render_gutter_utility: Some(PtrEq(Rc::new(|_: &dyn Fn() -> _| Some(span("u"))))),
        hunk_separators: Some(FileDiffHunkSeparators::Custom(PtrEq(Rc::new(
            |_: &crate::types::lines::HunkData| Some(span("separator")),
        )))),
        ..FileDiffOptions::default()
    }
}

/// The nodes under `node`: its children and its shadow root, recursively.
fn subtree(dom: &FakeDom, node: NodeKey, into: &mut Vec<NodeKey>) {
    let shadow_root = dom.doc.shadow_root(node);
    for child in dom.doc.child_nodes(node).into_iter().chain(shadow_root) {
        into.push(child);
        subtree(dom, child, into);
    }
}

fn render_everything(instance: &Instance, container: Option<NodeKey>, round: usize) {
    let (old_file, new_file) = pair(round);
    let annotations = Rc::new(
        [
            (AnnotationSide::Additions, 3),
            (AnnotationSide::Deletions, 9),
        ]
        .map(|(side, line_number)| DiffLineAnnotation {
            side,
            line_number,
            metadata: Some(1),
        })
        .to_vec(),
    );
    assert!(instance.lifecycle().render(FileDiffRenderProps {
        old_file: Some(Some(old_file)),
        new_file: Some(Some(new_file)),
        file_container: container,
        line_annotations: Some(annotations),
        render_range: Some(RenderRange {
            starting_line: 4.0,
            total_lines: 12.0,
            buffer_before: 20.0,
            buffer_after: 30.0,
        }),
        ..FileDiffRenderProps::default()
    }));
}

#[test]
fn clean_up_frees_every_node_the_instance_created_but_the_file_container() {
    for recycle in [false, true] {
        let dom = Rc::new(FakeDom::default());
        let wrapper = dom.doc.create_element("div");
        let container = dom.doc.create_element("diffs-container");
        let instance = instance(&dom, options_that_create_everything());
        render_everything(&instance, Some(container), 7);
        // Render once more with another diff so rows, slots and annotations
        // have been replaced at least once.
        render_everything(&instance, Some(container), 8);
        dom.doc.append_child(wrapper, container);

        let shadow_root = dom.doc.shadow_root(container).expect("a shadow root");
        let mut created = Vec::new();
        subtree(&dom, container, &mut created);
        // Something of each kind was made: shadow children (sprite, style,
        // pre, buffers), slotted children (header slots, annotations,
        // separators, gutter utility).
        assert!(dom.doc.child_nodes(shadow_root).len() >= 4);
        assert!(dom.doc.child_nodes(container).len() >= 5);
        assert!(created.len() > 100);

        instance.lifecycle().clean_up(recycle);

        assert!(instance.state().retired.is_empty());
        assert!(dom.doc.exists(container), "the caller owns the container");
        assert!(
            dom.doc.exists(shadow_root),
            "the shadow root is the container's"
        );
        assert!(dom.doc.child_nodes(container).is_empty());
        assert!(dom.doc.child_nodes(shadow_root).is_empty());
        assert_eq!(
            dom.doc.parent(container),
            None,
            "an unmanaged instance detaches it"
        );
        for node in created.into_iter().filter(|node| *node != shadow_root) {
            assert!(
                !dom.doc.exists(node),
                "{node:?} outlived cleanUp (recycle {recycle})"
            );
        }
    }
}

#[test]
fn rendering_and_cleaning_up_forever_keeps_the_document_bounded() {
    let dom = Rc::new(FakeDom::default());
    let warm_up = 20;
    let mut warm_capacity = 0;
    for round in 0..300 {
        // Even rounds pass a container (the caller's to free), odd rounds let the
        // instance make its own (the instance's to free).
        let container = (round % 2 == 0).then(|| dom.doc.create_element("diffs-container"));
        let instance = instance(&dom, options_that_create_everything());
        render_everything(&instance, container, round);
        render_everything(&instance, container, round + 1);
        instance.lifecycle().clean_up(round % 3 == 0);
        if let Some(container) = container {
            dom.doc.discard(container);
        }
        if round == warm_up {
            warm_capacity = dom.doc.capacity();
        }
    }
    assert_eq!(
        dom.doc.capacity(),
        warm_capacity,
        "an instance leaked nodes past cleanUp"
    );
}

#[test]
fn clean_up_frees_a_container_the_instance_made_itself_and_only_that() {
    let dom = Rc::new(FakeDom::default());
    let first = instance(&dom, options_that_create_everything());
    render_everything(&first, None, 3);
    let container = first.state().file_container.expect("a container");
    let shadow_root = dom.doc.shadow_root(container).expect("a shadow root");
    first.lifecycle().clean_up(false);
    assert!(!dom.doc.exists(container));
    assert!(!dom.doc.exists(shadow_root));

    // A container the caller passed survives, never discarded.
    let passed = dom.doc.create_element("diffs-container");
    let second = instance(&dom, options_that_create_everything());
    render_everything(&second, Some(passed), 3);
    second.lifecycle().clean_up(false);
    assert!(dom.doc.exists(passed));
}
