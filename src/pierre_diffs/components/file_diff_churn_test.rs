//! The ownership rule at the `FileDiff` level (no upstream counterpart): a
//! render replaces rows, columns, headers and slotted nodes and `retire`s them,
//! the managers' setup runs, `flush_managers` discards them; so rendering,
//! re-rendering with new files and partial rendering forever must keep the
//! `Document` at the size of one render. Test-only, original to this crate.

use std::rc::Rc;

use crate::components::file_diff::FileDiff;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::FileDiffRenderProps;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::hast;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::layout::RenderRange;
use crate::types::patch::FileContents;

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
