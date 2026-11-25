// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/includesFileAnnotations.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `Record<number, T[] | undefined>` is a `HashMap`; the `-1` hunk/line indexes are `i64` constants because `AnnotationSpan` indexes are signed upstream for this one case.

use std::collections::HashMap;

use crate::types::layout::RenderRange;

pub const FILE_ANNOTATION_LINE_NUMBER: usize = 0;
pub const FILE_ANNOTATION_HUNK_INDEX: i64 = -1;
pub const FILE_ANNOTATION_LINE_INDEX: i64 = -1;

/// `FILE_ANNOTATION_DOM_KEY`
pub fn file_annotation_dom_key() -> String {
    format!("{FILE_ANNOTATION_HUNK_INDEX},{FILE_ANNOTATION_LINE_INDEX}")
}

pub fn includes_file_annotations(line_numbers: impl IntoIterator<Item = usize>) -> bool {
    line_numbers
        .into_iter()
        .any(|line_number| line_number == FILE_ANNOTATION_LINE_NUMBER)
}

pub fn get_file_annotations<Annotation>(
    annotations: &HashMap<usize, Vec<Annotation>>,
) -> Option<&Vec<Annotation>> {
    annotations
        .get(&FILE_ANNOTATION_LINE_NUMBER)
        .filter(|file_annotations| !file_annotations.is_empty())
}

pub fn should_render_file_annotations(render_range: &RenderRange) -> bool {
    render_range.starting_line == FILE_ANNOTATION_LINE_NUMBER as f64
        && render_range.total_lines > 0.0
}
