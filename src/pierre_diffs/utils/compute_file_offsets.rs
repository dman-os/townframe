// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/computeFileOffsets.ts
// (upstream's `computeLineOffsets` test lives in test/computeLineOffsets.test.ts).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: offsets are UTF-8 byte offsets into the `&str` (they exist to slice
// it; JavaScript's are UTF-16 indexes), so a caller that needs JavaScript's
// units converts with `crate::utf16`. Upstream's two-strategy scan (native
// `indexOf` for source-like lines, a character scan once line breaks are
// dense) is a V8 performance tuning with identical results; a single byte scan
// replaces both.

/// Computes line start offsets for a string: `\n`, `\r` and `\r\n` each end a
/// line (a `\r\n` pair is one break); a trailing break produces a final empty
/// line start at `contents.len()`.
pub fn compute_line_offsets(contents: &str) -> Vec<usize> {
    let bytes = contents.as_bytes();
    let mut offsets = vec![0];
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\r' if bytes.get(index + 1) == Some(&b'\n') => {
                index += 2;
                offsets.push(index);
            }
            b'\r' | b'\n' => {
                index += 1;
                offsets.push(index);
            }
            _ => index += 1,
        }
    }
    offsets
}

/// Counts line breaks in a string, treating `\n`, `\r`, and `\r\n` the same way
/// [`compute_line_offsets`] does (a `\r\n` pair is one break), without building
/// an offsets array.
pub fn count_line_breaks(contents: &str) -> usize {
    let bytes = contents.as_bytes();
    let mut count = 0;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
            // Skip the `\n` of a `\r\n` pair so it counts as one break, not two.
            index += 1;
            count += 1;
        } else if matches!(bytes[index], b'\r' | b'\n') {
            count += 1;
        }
        index += 1;
    }
    count
}

/// Splits file contents into lines aligned with [`compute_line_offsets`].
/// Unlike `splitFileContents`, a trailing newline produces a final empty line.
pub fn lines_from_file_contents(contents: &str) -> Vec<&str> {
    let offsets = compute_line_offsets(contents);
    (0..offsets.len())
        .map(|index| {
            let start = offsets[index];
            let end = offsets.get(index + 1).copied().unwrap_or(contents.len());
            &contents[start..end]
        })
        .collect()
}
