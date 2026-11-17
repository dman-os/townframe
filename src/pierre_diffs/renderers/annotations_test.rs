// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/annotations.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; the renderer runs over the plain-text stub
// highlighter (`asyncRender` is `render`). `fileOld`/`fileNew` (the demo
// fixtures, absent from the extracted tree) are `big_fixture_files`, so the
// hard-coded line numbers are remapped to lines of that pair with the same
// roles: context line 5, added line 8, deleted old line 302 (upstream 44),
// a collapsed-region line 60 (upstream 15) and a line in the trailing
// collapsed region, 600. The `toMatchSnapshot` placement records are replaced
// by the invariants the loops around them already assert (each annotation
// follows its own line, every annotation is found once).

use std::collections::BTreeSet;

use crate::hast::Element;
use crate::hast::ElementContent;
use crate::renderers::diff_hunks_renderer::DiffHunksRenderer;
use crate::renderers::test_support::render;
use crate::renderers::test_support::render_in;
use crate::renderers::test_support::renderer_with;
use crate::renderers::test_support::split_renderer;
use crate::renderers::test_support::unified_renderer;
use crate::renderers::test_support::window;
use crate::test_utils::hast::annotation_index_of;
use crate::test_utils::hast::annotation_projection;
use crate::test_utils::hast::big_fixture_diff;
use crate::test_utils::hast::collect_all_elements;
use crate::test_utils::hast::diff_of;
use crate::test_utils::hast::find_slot_elements;
use crate::test_utils::hast::has_prop;
use crate::test_utils::hast::is_annotation_element;
use crate::test_utils::hast::is_line_element;
use crate::test_utils::hast::line_index_of;
use crate::test_utils::hast::line_type_of;
use crate::test_utils::hast::prop_str;
use crate::types::options::DiffStyle;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;

fn annotation(
    side: AnnotationSide,
    line_number: usize,
    metadata: &str,
) -> DiffLineAnnotation<String> {
    DiffLineAnnotation {
        side,
        line_number,
        metadata: Some(metadata.to_owned()),
    }
}

fn create_diff_with_leading_separator() -> FileDiffMetadata {
    let old: Vec<String> = (1..=40).map(|index| index.to_string()).collect();
    let new: Vec<String> = old
        .iter()
        .enumerate()
        .map(|(index, line)| {
            if index == 24 {
                String::from("changed-25")
            } else {
                line.clone()
            }
        })
        .collect();
    diff_of(
        "leading-separator.ts",
        &format!("{}\n", old.join("\n")),
        &format!("{}\n", new.join("\n")),
    )
}

fn create_no_hunk_diff() -> FileDiffMetadata {
    serde_json::from_value(serde_json::json!({
        "name": "renamed.ts",
        "prevName": "old-name.ts",
        "type": "rename-pure",
        "hunks": [],
        "splitLineCount": 0,
        "unifiedLineCount": 0,
        "isPartial": false,
        "deletionLines": [],
        "additionLines": [],
        "deletionCharacterCount": 0,
        "additionCharacterCount": 0,
    }))
    .expect("a no-hunk diff")
}

fn slot_names_of_element(element: &Element) -> Vec<String> {
    find_slot_elements(element)
        .into_iter()
        .map(|slot| {
            prop_str(slot, "name")
                .expect("slot should have a name")
                .to_owned()
        })
        .collect()
}

fn slot_names(node: &ElementContent) -> Vec<String> {
    node.as_element()
        .map(slot_names_of_element)
        .unwrap_or_default()
}

fn annotation_indexes(nodes: &[ElementContent]) -> Vec<String> {
    nodes
        .iter()
        .filter_map(|node| annotation_index_of(node).map(str::to_owned))
        .collect()
}

fn count_annotation_elements(ast: &[ElementContent]) -> usize {
    collect_all_elements(ast)
        .into_iter()
        .filter(|element| has_prop(element, "data-line-annotation"))
        .count()
}

fn with_annotations(
    renderer: &mut DiffHunksRenderer<String>,
    annotations: Vec<DiffLineAnnotation<String>>,
) {
    renderer.set_line_annotations(annotations);
}

fn expand_unchanged(style: DiffStyle) -> DiffHunksRenderer<String> {
    renderer_with(|options| {
        options.diff_style = Some(style);
        options.expand_unchanged = Some(true);
    })
}

mod file_level_annotations {
    use super::*;

    fn file_level() -> Vec<DiffLineAnnotation<String>> {
        vec![
            annotation(AnnotationSide::Deletions, 0, "old-file"),
            annotation(AnnotationSide::Additions, 0, "new-file"),
        ]
    }

    #[test]
    fn render_before_a_leading_hunk_separator_in_unified_style() {
        let mut renderer = unified_renderer::<String>();
        with_annotations(&mut renderer, file_level());

        let result = render(&renderer, &create_diff_with_leading_separator());
        let unified = result.unified_content_ast.expect("unifiedContentAST");
        let first_annotation_index = unified
            .iter()
            .position(is_annotation_element)
            .expect("an annotation");
        let first_separator_index = unified
            .iter()
            .position(|node| {
                node.as_element()
                    .is_some_and(|element| has_prop(element, "data-separator"))
            })
            .expect("a separator");

        assert_eq!(first_annotation_index, 0);
        assert!(first_separator_index > first_annotation_index);
        assert_eq!(
            annotation_index_of(&unified[first_annotation_index]),
            Some("-1,-1")
        );
        assert_eq!(
            slot_names(&unified[first_annotation_index]),
            ["annotation-deletions-0", "annotation-additions-0"]
        );
    }

    #[test]
    fn render_paired_top_rows_in_split_style() {
        let mut renderer = expand_unchanged(DiffStyle::Split);
        with_annotations(&mut renderer, file_level());

        let result = render(&renderer, &big_fixture_diff());
        let additions = result.additions_content_ast.expect("additionsContentAST");
        let deletions = result.deletions_content_ast.expect("deletionsContentAST");

        assert_eq!(annotation_index_of(&additions[0]), Some("-1,-1"));
        assert_eq!(annotation_index_of(&deletions[0]), Some("-1,-1"));
        assert_eq!(slot_names(&additions[0]), ["annotation-additions-0"]);
        assert_eq!(slot_names(&deletions[0]), ["annotation-deletions-0"]);
    }

    #[test]
    fn do_not_collide_with_first_row_annotation_keys_in_split_style() {
        let mut renderer = split_renderer::<String>();
        with_annotations(
            &mut renderer,
            vec![
                annotation(AnnotationSide::Deletions, 0, "old-file"),
                annotation(AnnotationSide::Additions, 0, "new-file"),
                annotation(AnnotationSide::Deletions, 1, "old-first-line"),
                annotation(AnnotationSide::Additions, 1, "new-first-line"),
            ],
        );

        let result = render(&renderer, &diff_of("first-row.ts", "old\n", "new\n"));
        let additions = result.additions_content_ast.expect("additionsContentAST");
        let deletions = result.deletions_content_ast.expect("deletionsContentAST");

        let indexes: Vec<String> = annotation_indexes(&deletions)
            .into_iter()
            .chain(annotation_indexes(&additions))
            .collect();

        assert_eq!(indexes.iter().filter(|index| *index == "-1,-1").count(), 2);
        assert_eq!(indexes.iter().filter(|index| *index == "0,0").count(), 2);
    }

    #[test]
    fn render_code_columns_for_no_hunk_diffs_with_only_file_level_annotations() {
        let mut renderer = split_renderer::<String>();
        with_annotations(&mut renderer, file_level());

        let result = render(&renderer, &create_no_hunk_diff());
        let additions = result.additions_content_ast.expect("additionsContentAST");
        let deletions = result.deletions_content_ast.expect("deletionsContentAST");

        assert_eq!(result.row_count, 1);
        assert_eq!(slot_names(&additions[0]), ["annotation-additions-0"]);
        assert_eq!(slot_names(&deletions[0]), ["annotation-deletions-0"]);
    }

    #[test]
    fn do_not_render_in_non_top_diff_render_chunks() {
        let mut renderer = unified_renderer::<String>();
        with_annotations(
            &mut renderer,
            vec![annotation(AnnotationSide::Additions, 0, "new-file")],
        );

        let result = render_in(&renderer, &big_fixture_diff(), &window(1, 5));
        let unified = result.unified_content_ast.expect("unifiedContentAST");

        assert!(
            !unified
                .iter()
                .any(|node| annotation_index_of(node) == Some("-1,-1"))
        );
    }
}

mod line_index_matching {
    use super::*;

    fn placement_annotations() -> Vec<DiffLineAnnotation<String>> {
        vec![
            annotation(AnnotationSide::Additions, 8, "new-import"),
            annotation(AnnotationSide::Additions, 30, "changed-line"),
            annotation(AnnotationSide::Deletions, 25, "old-line"),
        ]
    }

    #[test]
    fn annotation_line_index_matches_preceding_line_in_unified_style() {
        let annotations = placement_annotations();
        let mut renderer = expand_unchanged(DiffStyle::Unified);
        with_annotations(&mut renderer, annotations.clone());
        let result = render(&renderer, &big_fixture_diff());
        let unified = result.unified_content_ast.expect("unifiedContentAST");

        let mut found_annotation_count = 0;
        let mut last_line_element: Option<&Element> = None;
        // Iterate through all elements and verify each annotation follows its line
        for node in collect_all_elements(&unified) {
            if has_prop(node, "data-line") {
                last_line_element = Some(node);
                continue;
            }
            let Some(annotation_index) = prop_str(node, "data-line-annotation") else {
                continue;
            };
            let line_idx = annotation_index.split(',').nth(1).expect("line index");
            found_annotation_count += find_slot_elements(node).len();

            // The previous line element should be the line this annotation belongs to
            let previous = last_line_element.expect("lastLineElement");
            let previous_line_index = prop_str(previous, "data-line-index").expect("prevLineIndex");
            // In unified, the first value of data-line-index is the unified index
            assert_eq!(previous_line_index.split(',').next(), Some(line_idx));
        }
        assert_eq!(found_annotation_count, annotations.len());
        // The placement record (what the upstream snapshot holds) has one
        // entry per annotation row and each exposes its slots.
        let projection = annotation_projection(&unified);
        assert_eq!(
            projection
                .iter()
                .map(|entry| entry.slot_names.len())
                .sum::<usize>(),
            annotations.len()
        );
    }

    #[test]
    fn annotation_line_index_matches_preceding_line_in_split_style() {
        let annotations = placement_annotations();
        let total_additions = annotations
            .iter()
            .filter(|annotation| annotation.side == AnnotationSide::Additions)
            .count();
        let total_deletions = annotations.len() - total_additions;
        let mut renderer = expand_unchanged(DiffStyle::Split);
        with_annotations(&mut renderer, annotations);
        let result = render(&renderer, &big_fixture_diff());
        let additions = result.additions_content_ast.expect("additionsContentAST");
        let deletions = result.deletions_content_ast.expect("deletionsContentAST");

        let mut indexes: [BTreeSet<String>; 2] = [BTreeSet::new(), BTreeSet::new()];
        for (side, (ast, expected_count)) in
            [(&additions, total_additions), (&deletions, total_deletions)]
                .into_iter()
                .enumerate()
        {
            let mut found_count = 0;
            let mut last_line_node: Option<&Element> = None;
            for node in collect_all_elements(ast) {
                if has_prop(node, "data-line") {
                    last_line_node = Some(node);
                    continue;
                }
                let Some(annotation_index) = prop_str(node, "data-line-annotation") else {
                    continue;
                };
                assert!(
                    indexes[side].insert(annotation_index.to_owned()),
                    "Duplicate annotation index: {annotation_index}"
                );
                let slots = find_slot_elements(node);
                if slots.is_empty() {
                    // Empty annotation wrapper (for sync with other side)
                    continue;
                }
                found_count += slots.len();
                let line_idx = annotation_index.split(',').nth(1).expect("line index");
                let previous = last_line_node.expect("lastLineNode");
                let previous_line_index =
                    prop_str(previous, "data-line-index").expect("prevLineIndex");
                assert_eq!(previous_line_index.split(',').nth(1), Some(line_idx));
            }
            assert_eq!(found_count, expected_count);
        }

        // Verify both sides have matching annotation indices
        assert_eq!(indexes[0], indexes[1]);
        assert_eq!(
            annotation_projection(&additions).len(),
            annotation_projection(&deletions).len()
        );
    }
}

mod annotations_in_different_line_types {
    use super::*;

    // Line 5 is context, line 8 is addition, old line 302 is deleted. Line 60
    // is in the collapsed region between the 4th and 5th hunks and line 600 in
    // the last collapsed region (trailing the final hunk).
    fn typed_annotations() -> Vec<DiffLineAnnotation<String>> {
        vec![
            annotation(AnnotationSide::Additions, 5, "context"),
            annotation(AnnotationSide::Additions, 8, "change-addition"),
            annotation(AnnotationSide::Deletions, 302, "change-deletion"),
            annotation(AnnotationSide::Additions, 60, "context-expanded"),
            annotation(AnnotationSide::Additions, 600, "context-expanded"),
        ]
    }

    fn expected_type(slot_name: &str) -> &'static str {
        match slot_name {
            "annotation-additions-5" => "context",
            "annotation-additions-8" => "change-addition",
            "annotation-deletions-302" => "change-deletion",
            // Final expanded content region, since that code is rendered
            // through a slightly different code page
            "annotation-additions-60" | "annotation-additions-600" => "context-expanded",
            other => panic!("unexpected slot {other}"),
        }
    }

    #[test]
    fn annotations_on_all_line_types_context_addition_deletion_expanded() {
        let mut renderer = expand_unchanged(DiffStyle::Unified);
        with_annotations(&mut renderer, typed_annotations());
        let result = render(&renderer, &big_fixture_diff());
        let unified = result.unified_content_ast.expect("unifiedContentAST");
        assert_eq!(count_annotation_elements(&unified), 5);

        // Iterate and verify each annotation's preceding line type
        for index in 1..unified.len() {
            if !is_annotation_element(&unified[index]) {
                continue;
            }
            let slots = slot_names(&unified[index]);
            let slot_name = slots
                .first()
                .expect("there should always be slots in unifiedAST");
            assert_eq!(
                line_type_of(&unified[index - 1]),
                Some(expected_type(slot_name))
            );
        }
    }

    #[test]
    fn annotations_on_all_line_types_in_split_style() {
        let mut renderer = expand_unchanged(DiffStyle::Split);
        with_annotations(&mut renderer, typed_annotations());
        let result = render(&renderer, &big_fixture_diff());
        let additions = result.additions_content_ast.expect("additionsContentAST");
        let deletions = result.deletions_content_ast.expect("deletionsContentAST");

        // Additions AST: lines 5 (context), 8 (change-addition), 60 and 600
        // (expanded); Deletions AST: old line 302 (change-deletion)
        for (ast, expected_count) in [(&additions, 4), (&deletions, 1)] {
            let mut count = 0;
            let mut last_line: Option<&Element> = None;
            for node in collect_all_elements(ast) {
                if has_prop(node, "data-line") {
                    last_line = Some(node);
                    continue;
                }
                if !has_prop(node, "data-line-annotation") {
                    continue;
                }
                let slots = slot_names_of_element(node);
                // Skip empty annotation wrappers
                let Some(slot_name) = slots.first() else {
                    continue;
                };
                count += 1;
                let previous = last_line.expect("lastLine");
                assert_eq!(
                    prop_str(previous, "data-line-type"),
                    Some(expected_type(slot_name))
                );
            }
            assert_eq!(count, expected_count);
        }
    }
}

mod annotation_collapsing_in_unified_style {
    use super::*;

    #[test]
    fn annotations_on_both_addition_and_deletion_side_of_same_context_line_collapse_into_1_element()
    {
        let mut renderer = unified_renderer::<String>();
        with_annotations(
            &mut renderer,
            vec![
                annotation(AnnotationSide::Additions, 5, "annotation-from-additions"),
                annotation(AnnotationSide::Deletions, 5, "annotation-from-deletions"),
            ],
        );
        let result = render(&renderer, &big_fixture_diff());
        let unified = result.unified_content_ast.expect("unifiedContentAST");

        // Should only have 1 annotation element
        assert_eq!(count_annotation_elements(&unified), 1);

        // Find the annotation and verify it has 2 slots
        let annotation_element = collect_all_elements(&unified)
            .into_iter()
            .find(|element| has_prop(element, "data-line-annotation"))
            .expect("annotationEl");
        let slot_names = slot_names_of_element(annotation_element);
        assert_eq!(slot_names.len(), 2);
        assert!(slot_names.contains(&String::from("annotation-additions-5")));
        assert!(slot_names.contains(&String::from("annotation-deletions-5")));
    }

    #[test]
    fn in_split_style_annotations_on_both_sides_remain_separate() {
        let mut renderer = split_renderer::<String>();
        with_annotations(
            &mut renderer,
            vec![
                annotation(AnnotationSide::Additions, 5, "some-metadata"),
                annotation(AnnotationSide::Deletions, 5, "some-metadata"),
            ],
        );
        let result = render(&renderer, &big_fixture_diff());
        let additions = result.additions_content_ast.expect("additionsContentAST");
        let deletions = result.deletions_content_ast.expect("deletionsContentAST");

        // Each side should have 1 annotation
        assert_eq!(count_annotation_elements(&additions), 1);
        assert_eq!(count_annotation_elements(&deletions), 1);

        // Find annotations and verify each has 1 slot
        let find = |ast: &[ElementContent]| {
            slot_names_of_element(
                collect_all_elements(ast)
                    .into_iter()
                    .find(|element| has_prop(element, "data-line-annotation"))
                    .expect("annotation"),
            )
        };
        assert_eq!(find(&additions), ["annotation-additions-5"]);
        assert_eq!(find(&deletions), ["annotation-deletions-5"]);
    }
}

#[test]
fn line_elements_carry_their_line_index_for_the_helpers() {
    // The helpers the loops above rely on agree with the renderer's markup.
    let renderer = split_renderer::<String>();
    let result = render(&renderer, &diff_of("a.ts", "old\n", "new\n"));
    let additions = result.additions_content_ast.expect("additionsContentAST");
    let line = additions
        .iter()
        .find(|node| is_line_element(node))
        .expect("a line");
    assert_eq!(line_index_of(line), Some("1,0"));
}
