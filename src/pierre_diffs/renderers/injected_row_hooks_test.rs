// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/InjectedRowHooks.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; the subclasses overriding the `protected`
// hooks are `DiffHunksRendererHooks` implementations. The third upstream test
// (`UnresolvedFileHunksRenderer` emitting merge-conflict action rows) is not
// ported: the merge-conflict renderer is deferred (PARITY.md).

use crate::hast::ElementContent;
use crate::hast::Properties;
use crate::renderers::diff_hunks_renderer::DiffHunksRenderer;
use crate::renderers::diff_hunks_renderer::DiffHunksRendererHooks;
use crate::renderers::diff_hunks_renderer::DiffHunksRendererOptions;
use crate::renderers::diff_hunks_renderer::InjectedRow;
use crate::renderers::diff_hunks_renderer::RenderedLineContext;
use crate::renderers::diff_hunks_renderer::SplitInjectedRow;
use crate::renderers::diff_hunks_renderer::SplitInjectedRowPlacement;
use crate::renderers::diff_hunks_renderer::UnifiedInjectedRowPlacement;
use crate::renderers::test_support::render;
use crate::test_utils::hast::diff_of;
use crate::test_utils::hast::has_prop;
use crate::test_utils::hast::prop_number;
use crate::test_utils::hast::prop_str;
use crate::types::options::DiffStyle;
use crate::utils::render::get_line_annotation_name::AnnotationName;
use crate::utils::render::get_line_annotation_name::get_line_annotation_name;
use crate::utils::render::hast_utils::GutterBufferType;
use crate::utils::render::hast_utils::create_gutter_gap;
use crate::utils::render::hast_utils::create_hast_element;

fn create_injected_row(name: &str) -> InjectedRow {
    InjectedRow {
        content: create_hast_element(
            "div",
            Vec::new(),
            Properties::new().with("data-test-inline-row", name),
        ),
        gutter: create_gutter_gap(None, GutterBufferType::Annotation, 1),
    }
}

fn get_top_level_row_names(rows: &[ElementContent]) -> Vec<String> {
    rows.iter()
        .filter_map(|row| row.as_element())
        .filter_map(|element| prop_str(element, "data-test-inline-row").map(str::to_owned))
        .collect()
}

fn get_top_level_row_index(rows: &[ElementContent], key: &str) -> Option<usize> {
    rows.iter().position(|row| {
        row.as_element()
            .is_some_and(|element| prop_str(element, "data-test-inline-row") == Some(key))
    })
}

fn get_top_level_line_index(rows: &[ElementContent], line_number: i64) -> Option<usize> {
    rows.iter().position(|row| {
        row.as_element()
            .is_some_and(|element| prop_number(element, "data-line") == Some(line_number))
    })
}

fn get_top_level_buffer_index(rows: &[ElementContent]) -> Option<usize> {
    rows.iter().position(|row| {
        row.as_element()
            .is_some_and(|element| has_prop(element, "data-content-buffer"))
    })
}

fn renderer_with_hooks(
    diff_style: DiffStyle,
    hooks: impl DiffHunksRendererHooks + 'static,
) -> DiffHunksRenderer<()> {
    let mut options = DiffHunksRendererOptions::default();
    options.base.diff_style = Some(diff_style);
    DiffHunksRenderer::with_hooks(
        options,
        Box::new(|annotation| {
            get_line_annotation_name(AnnotationName::DiffLine {
                side: annotation.side,
                line_number: annotation.line_number,
            })
        }),
        Box::new(hooks),
    )
}

struct UnifiedInjectedRowTestRenderer;

impl DiffHunksRendererHooks for UnifiedInjectedRowTestRenderer {
    fn unified_injected_rows_for_line(
        &self,
        context: &RenderedLineContext,
    ) -> Option<UnifiedInjectedRowPlacement> {
        if context.addition_line.map(|line| line.line_number) != Some(1) {
            return None;
        }
        Some(UnifiedInjectedRowPlacement {
            before: Some(vec![create_injected_row("unified-before-line-1")]),
            after: None,
        })
    }
}

struct SplitInjectedRowTestRenderer;

impl DiffHunksRendererHooks for SplitInjectedRowTestRenderer {
    fn split_injected_rows_for_line(
        &self,
        context: &RenderedLineContext,
    ) -> Option<SplitInjectedRowPlacement> {
        if context.split_line_index != 0 {
            return None;
        }
        Some(SplitInjectedRowPlacement {
            before: Some(vec![
                SplitInjectedRow {
                    deletion: Some(create_injected_row("split-deletion-only")),
                    addition: None,
                },
                SplitInjectedRow {
                    deletion: Some(create_injected_row("split-deletion-paired")),
                    addition: Some(create_injected_row("split-addition-paired")),
                },
            ]),
            after: None,
        })
    }
}

#[test]
fn unified_hook_inserts_before_rows_before_the_triggering_line() {
    let renderer = renderer_with_hooks(DiffStyle::Unified, UnifiedInjectedRowTestRenderer);
    let diff = diff_of(
        "file.ts",
        "const a = 1;\nconst b = 2;\n",
        "const a = 1;\nconst c = 3;\n",
    );

    let result = render(&renderer, &diff);

    let unified = result
        .unified_content_ast
        .expect("expected unified content AST");
    let line_index = get_top_level_line_index(&unified, 1).expect("line 1");
    let inline_row_index =
        get_top_level_row_index(&unified, "unified-before-line-1").expect("injected row");

    assert_eq!(result.row_count, diff.unified_line_count + 1);
    assert_eq!(inline_row_index + 1, line_index);
}

#[test]
fn split_hook_preserves_one_sided_buffering_before_later_paired_rows() {
    let renderer = renderer_with_hooks(DiffStyle::Split, SplitInjectedRowTestRenderer);
    let diff = diff_of(
        "file.ts",
        "const a = 1;\nconst b = 2;\nconst e = 2;\n",
        "const a = 1;\nconst c = 3;\n",
    );

    let result = render(&renderer, &diff);

    let deletions = result
        .deletions_content_ast
        .expect("expected deletions content AST");
    let additions = result
        .additions_content_ast
        .expect("expected additions content AST");

    assert_eq!(result.row_count, diff.split_line_count + 2);
    assert_eq!(
        get_top_level_row_names(&deletions),
        ["split-deletion-only", "split-deletion-paired"]
    );
    assert_eq!(
        get_top_level_row_names(&additions),
        ["split-addition-paired"]
    );

    let addition_buffer_index = get_top_level_buffer_index(&additions).expect("a buffer");
    let addition_paired_index =
        get_top_level_row_index(&additions, "split-addition-paired").expect("paired addition");
    let deletion_only_index =
        get_top_level_row_index(&deletions, "split-deletion-only").expect("deletion only");
    let deletion_paired_index =
        get_top_level_row_index(&deletions, "split-deletion-paired").expect("paired deletion");

    assert!(addition_paired_index > addition_buffer_index);
    assert!(deletion_paired_index > deletion_only_index);
}
