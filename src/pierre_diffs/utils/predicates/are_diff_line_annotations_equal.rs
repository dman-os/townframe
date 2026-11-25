// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areDiffLineAnnotationsEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: JavaScript `===` on `metadata` is `PartialEq` (upstream compares by identity; metadata is an opaque value, so structural equality is the nearest Rust meaning and can only report more pairs equal).

use crate::types::view::DiffLineAnnotation;

pub fn are_diff_line_annotations_equal<Metadata: PartialEq>(
    annotation_a: &DiffLineAnnotation<Metadata>,
    annotation_b: &DiffLineAnnotation<Metadata>,
) -> bool {
    annotation_a.line_number == annotation_b.line_number
        && annotation_a.side == annotation_b.side
        && annotation_a.metadata == annotation_b.metadata
}
