// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getHunkSideBoundaries.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: subtraction saturates at 0 where JavaScript yields -1 for a malformed `@@ -0,N` header (external input must not panic).

/// Converts a unified hunk side's start/count into its consumed-file range.
pub fn get_hunk_side_start_boundary(start: usize, count: usize) -> usize {
    start.saturating_sub(if count == 0 { 0 } else { 1 })
}

pub fn get_hunk_side_end_boundary(start: usize, count: usize) -> usize {
    get_hunk_side_start_boundary(start, count) + count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_hunk_side_boundaries_follow_the_unified_zero_count_quirk() {
        assert_eq!(get_hunk_side_start_boundary(5, 3), 4);
        assert_eq!(get_hunk_side_end_boundary(5, 3), 7);
        assert_eq!(get_hunk_side_start_boundary(5, 0), 5);
        assert_eq!(get_hunk_side_end_boundary(5, 0), 5);
    }
}
