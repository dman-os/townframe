// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getLineAnnotationName.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the `LineAnnotation | DiffLineAnnotation` union is `AnnotationName`: a line annotation has no side, a diff annotation has one.

use crate::types::view::AnnotationSide;

/// Which kind of annotation a slot name is derived from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationName {
    /// `LineAnnotation`: no side.
    Line { line_number: usize },
    /// `DiffLineAnnotation`
    DiffLine {
        side: AnnotationSide,
        line_number: usize,
    },
}

pub fn get_line_annotation_name(annotation: AnnotationName) -> String {
    match annotation {
        AnnotationName::Line { line_number } => format!("annotation-{line_number}"),
        AnnotationName::DiffLine { side, line_number } => {
            let side = match side {
                AnnotationSide::Deletions => "deletions",
                AnnotationSide::Additions => "additions",
            };
            format!("annotation-{side}-{line_number}")
        }
    }
}
