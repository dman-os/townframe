// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/trimPatchContext.ts
// (and packages/diffs/src/utils/appendItems.ts, which is `Vec::extend`: it exists
// upstream only to dodge engine argument-count limits on `push(...items)`).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: (.*))?/m` is
// `scanners::match_hunk_header`; `parseInt` of a digit string that overflows
// `usize` counts as upstream's `isNaN` (the header line is kept verbatim).

use crate::scanners::match_hunk_header;

struct CurrentHunk {
    addition_start: usize,
    deletion_start: usize,
    addition_count: usize,
    deletion_count: usize,
    hunk_lines: Vec<String>,
    context_lines: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ContextFlushMode {
    BeforeChange,
    Leading,
    Trailing,
}

/// Default `contextSize` of upstream's `trimPatchContext`.
pub const DEFAULT_TRIM_CONTEXT_SIZE: usize = 10;

/// A utility function to trim out excess context lines from a patch file. It
/// will maintain line numbers, and properly update the hunk context markers, as
/// well as be able to create new hunks where necessary if there's excessive
/// context between changes
pub fn trim_patch_context(patch: &str, context_size: usize) -> String {
    let mut lines: Vec<String> = Vec::new();

    let mut current_hunk: Option<CurrentHunk> = None;
    for line in patch.split('\n') {
        // We've come across a new hunk boundary
        if let Some(parsed_hunk_header) = match_hunk_header(line) {
            // If we have an existing hunk, lets close it out
            // before setting up a new one
            if let Some(mut hunk) = current_hunk.take()
                && !hunk.hunk_lines.is_empty()
            {
                flush_context_lines(&mut hunk, context_size, ContextFlushMode::Trailing);
                flush_hunk(&hunk, &mut lines);
            }

            let addition_start = parsed_hunk_header.new_start.parse::<usize>();
            let deletion_start = parsed_hunk_header.old_start.parse::<usize>();
            let addition_count = parsed_hunk_header
                .new_count
                .map_or(Ok(1), str::parse::<usize>);
            let deletion_count = parsed_hunk_header
                .old_count
                .map_or(Ok(1), str::parse::<usize>);

            // If we can't parse valid numbers out of the hunk header
            // lets just skip the hunk altogether
            match (
                addition_start,
                deletion_start,
                addition_count,
                deletion_count,
            ) {
                (Ok(addition_start), Ok(deletion_start), Ok(_), Ok(_)) => {
                    current_hunk = Some(CurrentHunk {
                        addition_start,
                        deletion_start,
                        addition_count: 0,
                        deletion_count: 0,
                        hunk_lines: Vec::new(),
                        context_lines: Vec::new(),
                    });
                }
                _ => lines.push(line.to_owned()),
            }
            continue;
        }

        // If we don't have a current hunk, then we should just assume this is
        // general metadata
        let Some(hunk) = current_hunk.as_mut() else {
            lines.push(line.to_owned());
            continue;
        };

        // If we are dealing with a context line...
        if line.starts_with(' ') {
            hunk.context_lines.push(line.to_owned());
        } else if !line.is_empty() {
            if !hunk.hunk_lines.is_empty() && hunk.context_lines.len() > context_size * 2 {
                let omitted_context_line_count = hunk.context_lines.len() - context_size * 2;
                // `contextLines.slice(-contextSize)`: with `contextSize` 0 that is
                // `slice(-0)`, which JavaScript treats as `slice(0)` -- the whole
                // array -- so every context line carries into the next hunk.
                let next_context_lines = if context_size == 0 {
                    hunk.context_lines.clone()
                } else {
                    hunk.context_lines[hunk.context_lines.len() - context_size..].to_vec()
                };
                flush_context_lines(hunk, context_size, ContextFlushMode::Trailing);
                let emitted_addition_count = hunk.addition_count;
                let emitted_deletion_count = hunk.deletion_count;
                flush_hunk(hunk, &mut lines);

                *hunk = CurrentHunk {
                    addition_start: hunk.addition_start
                        + emitted_addition_count
                        + omitted_context_line_count,
                    deletion_start: hunk.deletion_start
                        + emitted_deletion_count
                        + omitted_context_line_count,
                    deletion_count: 0,
                    addition_count: 0,
                    context_lines: next_context_lines,
                    hunk_lines: Vec::new(),
                };
            }

            flush_context_lines(
                hunk,
                context_size,
                if hunk.hunk_lines.is_empty() {
                    ContextFlushMode::Leading
                } else {
                    ContextFlushMode::BeforeChange
                },
            );
            hunk.hunk_lines.push(line.to_owned());
            if line.starts_with('+') {
                hunk.addition_count += 1;
            } else if line.starts_with('-') {
                hunk.deletion_count += 1;
            }
        }
    }

    if let Some(mut hunk) = current_hunk
        && !hunk.hunk_lines.is_empty()
    {
        flush_context_lines(&mut hunk, context_size, ContextFlushMode::Trailing);
        flush_hunk(&hunk, &mut lines);
    }

    let result = lines.join("\n");
    if patch.ends_with('\n') {
        format!("{result}\n")
    } else {
        result
    }
}

fn flush_context_lines(hunk: &mut CurrentHunk, context_size: usize, mode: ContextFlushMode) {
    if mode == ContextFlushMode::Leading && hunk.context_lines.len() > context_size {
        let difference = hunk.context_lines.len() - context_size;
        hunk.context_lines.drain(..difference);
        hunk.addition_start += difference;
        hunk.deletion_start += difference;
    }

    if mode == ContextFlushMode::Trailing && hunk.context_lines.len() > context_size {
        hunk.context_lines.truncate(context_size);
    }

    if !hunk.context_lines.is_empty() {
        let count = hunk.context_lines.len();
        hunk.hunk_lines.append(&mut hunk.context_lines);
        hunk.addition_count += count;
        hunk.deletion_count += count;
    }
}

fn flush_hunk(hunk: &CurrentHunk, lines: &mut Vec<String>) {
    lines.push(format!(
        "@@ -{} +{} @@",
        format_hunk_range(hunk.deletion_start, hunk.deletion_count),
        format_hunk_range(hunk.addition_start, hunk.addition_count)
    ));
    lines.extend(hunk.hunk_lines.iter().cloned());
}

fn format_hunk_range(start: usize, count: usize) -> String {
    if count == 1 {
        start.to_string()
    } else {
        format!("{start},{count}")
    }
}
