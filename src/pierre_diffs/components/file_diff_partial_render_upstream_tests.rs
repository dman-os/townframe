// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/FileDiff.partialRender.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test and jsdom are replaced by nextest and FakeDom; rendering is synchronous, so async waits are omitted.

use std::rc::Rc;


use crate::components::file_diff::FileDiffRenderProps;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::types::layout::RenderRange;
use crate::types::options::DiffStyle;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;

use super::file_diff_upstream_tests::{file, instance, parse, shadow_root, window, Instance, Options};

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
    instance.lifecycle().render(FileDiffRenderProps {
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

    instance.lifecycle().render(FileDiffRenderProps {
        file_container: Some(file_container),
        old_file: Some(None),
        new_file: Some(Some(super::file_diff_upstream_tests::file(
            "new.txt",
            "alpha\nbeta\n",
        ))),
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

    instance.lifecycle().render(FileDiffRenderProps {
        file_container: Some(file_container),
        old_file: Some(Some(super::file_diff_upstream_tests::file(
            "deleted.txt",
            "alpha\nbeta\n",
        ))),
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
