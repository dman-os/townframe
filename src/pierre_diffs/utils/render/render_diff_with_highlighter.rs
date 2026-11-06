// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/renderDiffWithHighlighter.ts
// (and the hast-bearing result types of packages/diffs/src/types.ts:
// `RenderDiffFilesResult`, `ThemedDiffResult`).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `lazyLineAST` and `setDeferredArrayItem` are dropped (rows are built
// eagerly) and `useTokenTransformer` is not supported (see `render_token_lines`).
// jsdiff's `diffChars`/`diffWordsWithSpace` are `crate::diff` (similar-backed),
// so word- and char-diff decoration ranges can differ from jsdiff's where
// `similar` tokenises or aligns differently; `lineDiffType: none` output is
// identical. The result lines are `Vec<Option<ElementContent>>` because a
// windowed render writes rows at their absolute indexes and leaves the others
// as holes, like the sparse JavaScript arrays upstream builds.

use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::diff::ChangeObject;
use crate::diff::diff_chars;
use crate::diff::diff_words_with_space;
use crate::hast::ElementContent;
use crate::highlighter_types::DiffsHighlighter;
use crate::highlighter_types::ThemedToken;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::ForceDiffPlainTextOptions;
use crate::types::lines::LineIndex;
use crate::types::lines::LineInfo;
use crate::types::lines::LineTypes;
use crate::types::options::BaseThemeType;
use crate::types::options::LineDiffTypes;
use crate::types::options::RenderDiffOptions;
use crate::types::options::ThemeOption;
use crate::types::patch::FileDiffMetadata;
use crate::utf16;
use crate::utils::clean_last_newline::clean_last_newline;
use crate::utils::get_filetype_from_file_name::get_filetype_from_file_name;
use crate::utils::iterate_over_diff::DiffLineType;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;
use crate::utils::parse_diff_decorations::CreateDiffSpanDecorationProps;
use crate::utils::parse_diff_decorations::DecorationItem;
use crate::utils::parse_diff_decorations::PushOrJoinSpanProps;
use crate::utils::parse_diff_decorations::Span;
use crate::utils::parse_diff_decorations::create_diff_span_decoration;
use crate::utils::parse_diff_decorations::push_or_join_span;
use crate::utils::render::get_highlighter_theme_styles::get_highlighter_theme_styles;
use crate::utils::render::get_token_options::get_token_options;
use crate::utils::render::render_token_lines::MergeWhitespaces;
use crate::utils::render::render_token_lines::RenderTokenLinesOptions;
use crate::utils::render::render_token_lines::render_token_lines;

/// `RenderDiffFilesResult`
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RenderDiffFilesResult {
    pub deletion_lines: Vec<Option<ElementContent>>,
    pub addition_lines: Vec<Option<ElementContent>>,
}

/// `ThemedDiffResult`
#[derive(Debug, Clone, PartialEq)]
pub struct ThemedDiffResult {
    pub code: RenderDiffFilesResult,
    pub theme_styles: String,
    pub base_theme_type: Option<BaseThemeType>,
}

pub fn render_diff_with_highlighter(
    diff: &FileDiffMetadata,
    highlighter: &dyn DiffsHighlighter,
    options: &RenderDiffOptions,
    plain_text: &ForceDiffPlainTextOptions,
) -> ThemedDiffResult {
    assert!(
        !options.use_token_transformer,
        "useTokenTransformer (the editor token transformer) is not ported"
    );
    let force_plain_text = plain_text.force_plain_text;
    let (starting_line, total_lines) = if force_plain_text {
        (
            plain_text.starting_line.unwrap_or(0),
            plain_text.total_lines,
        )
    } else {
        // Tokenization must include preceding lines to preserve lexical state.
        (0, None)
    };
    let collapsed_context_threshold = plain_text
        .collapsed_context_threshold
        .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD);
    let is_windowed_highlight = starting_line > 0 || total_lines.is_some();
    let base_theme_type = match &options.theme {
        ThemeOption::Name(name) => highlighter.get_theme(name).theme_type,
        ThemeOption::Themes(_) => None,
    };
    let theme_styles = get_highlighter_theme_styles(&options.theme, highlighter, None);

    // If we have a large file and we are rendering the WHOLE plain diff ast,
    // then we should remove the lineDiffType to make sure things render quickly.
    // For highlighted ASTs or windowed highlights, we should just inherit the
    // setting
    let line_diff_type = if force_plain_text
        && !is_windowed_highlight
        && (diff.unified_line_count > 1000 || diff.split_line_count > 1000)
    {
        LineDiffTypes::None
    } else {
        options.line_diff_type
    };

    let should_group_all = !force_plain_text && !diff.is_partial;
    let expanded_hunks_for_iteration = if force_plain_text {
        plain_text.expanded_hunks.as_ref()
    } else {
        None
    };
    // Insertion-ordered, like the `Map` upstream iterates.
    let mut buckets: Vec<(usize, RenderBucket)> = Vec::new();

    let all = ExpandedHunks::All;
    iterate_over_diff(
        IterateOverDiffProps {
            diff,
            diff_style: IterationDiffStyle::Both,
            starting_line,
            total_lines,
            expanded_hunks: if is_windowed_highlight {
                expanded_hunks_for_iteration
            } else {
                Some(&all)
            },
            collapsed_context_threshold,
        },
        |props| {
            let key = if should_group_all {
                0
            } else {
                props.hunk_index
            };
            let position = match buckets.iter().rposition(|(existing, _)| *existing == key) {
                Some(position) => position,
                None => {
                    buckets.push((key, RenderBucket::default()));
                    buckets.len() - 1
                }
            };
            let bucket = &mut buckets[position].1;
            let split_line_index = props.addition_line.map_or_else(
                || {
                    props
                        .deletion_line
                        .expect("a row has a side")
                        .split_line_index
                },
                |line| line.split_line_index,
            );

            if props.line_type == DiffLineType::Change
                && let (Some(addition_line), Some(deletion_line)) =
                    (props.addition_line, props.deletion_line)
            {
                compute_line_diff_decorations(
                    &diff.deletion_lines[deletion_line.line_index],
                    &diff.addition_lines[addition_line.line_index],
                    bucket,
                    line_diff_type,
                    options.max_line_diff_length,
                );
            }

            if let Some(deletion_line) = props.deletion_line {
                append_content(
                    &diff.deletion_lines[deletion_line.line_index],
                    deletion_line.line_index,
                    is_windowed_highlight,
                    &mut bucket.deletion_segments,
                    &mut bucket.deletion_content,
                );
                bucket.deletion_info.push(Some(LineInfo {
                    line_type: change_or(props.line_type, LineTypes::ChangeDeletion),
                    line_number: deletion_line.line_number,
                    alt_line_number: if props.line_type == DiffLineType::Change {
                        None
                    } else {
                        props.addition_line.map(|line| line.line_number)
                    },
                    line_index: LineIndex::Pair(deletion_line.unified_line_index, split_line_index),
                }));
            }

            if let Some(addition_line) = props.addition_line {
                append_content(
                    &diff.addition_lines[addition_line.line_index],
                    addition_line.line_index,
                    is_windowed_highlight,
                    &mut bucket.addition_segments,
                    &mut bucket.addition_content,
                );
                bucket.addition_info.push(Some(LineInfo {
                    line_type: change_or(props.line_type, LineTypes::ChangeAddition),
                    line_number: addition_line.line_number,
                    alt_line_number: if props.line_type == DiffLineType::Change {
                        None
                    } else {
                        props.deletion_line.map(|line| line.line_number)
                    },
                    line_index: LineIndex::Pair(addition_line.unified_line_index, split_line_index),
                }));
            }
            false
        },
    );

    let code = render_buckets(
        diff,
        highlighter,
        options,
        force_plain_text,
        should_group_all,
        &buckets,
    );

    ThemedDiffResult {
        code,
        theme_styles,
        base_theme_type,
    }
}

/// Highlights each bucket's accumulated content and places the rendered
/// lines at their absolute line index (windowed renders leave holes).
fn render_buckets(
    diff: &FileDiffMetadata,
    highlighter: &dyn DiffsHighlighter,
    options: &RenderDiffOptions,
    force_plain_text: bool,
    should_group_all: bool,
    buckets: &[(usize, RenderBucket)],
) -> RenderDiffFilesResult {
    let mut code = RenderDiffFilesResult::default();
    for (_, bucket) in buckets {
        if bucket.deletion_content.length == 0 && bucket.addition_content.length == 0 {
            continue;
        }

        let deletion_name = diff.prev_name.as_deref().unwrap_or(&diff.name);
        let (deletion_lines, addition_lines) = render_two_files(
            highlighter,
            options,
            force_plain_text.then_some("text").or(diff.lang.as_deref()),
            (
                deletion_name,
                &bucket.deletion_content.value,
                &bucket.deletion_info,
                &bucket.deletion_decorations,
            ),
            (
                &diff.name,
                &bucket.addition_content.value,
                &bucket.addition_info,
                &bucket.addition_decorations,
            ),
        );

        if should_group_all {
            code.deletion_lines = deletion_lines.into_iter().map(Some).collect();
            code.addition_lines = addition_lines.into_iter().map(Some).collect();
            continue;
        }

        for (target, lines, segments) in [
            (
                &mut code.deletion_lines,
                deletion_lines,
                &bucket.deletion_segments,
            ),
            (
                &mut code.addition_lines,
                addition_lines,
                &bucket.addition_segments,
            ),
        ] {
            if segments.is_empty() {
                target.extend(lines.into_iter().map(Some));
            } else {
                let mut lines: Vec<Option<ElementContent>> = lines.into_iter().map(Some).collect();
                for segment in segments {
                    for index in 0..segment.count {
                        let absolute = segment.target_index + index;
                        if target.len() <= absolute {
                            target.resize(absolute + 1, None);
                        }
                        target[absolute] = lines[segment.original_offset + index].take();
                    }
                }
            }
        }
    }
    code
}

/// `type === 'change' ? change : type` for the line info of one side.
fn change_or(line_type: DiffLineType, change: LineTypes) -> LineTypes {
    match line_type {
        DiffLineType::Change => change,
        DiffLineType::Context => LineTypes::Context,
        DiffLineType::ContextExpanded => LineTypes::ContextExpanded,
    }
}

fn append_content(
    line_content: &str,
    line_index: usize,
    is_windowed_highlight: bool,
    segments: &mut Vec<HighlightSegment>,
    content_wrapper: &mut FakeArray,
) {
    if is_windowed_highlight {
        let needs_new_segment = segments
            .last()
            .is_none_or(|segment| segment.target_index + segment.count != line_index);
        if needs_new_segment {
            segments.push(HighlightSegment {
                target_index: line_index,
                original_offset: content_wrapper.length,
                count: 0,
            });
        }
        segments.last_mut().expect("just ensured").count += 1;
    }
    content_wrapper.push(line_content);
}

fn push_word_line_decorations(
    line_diff: &[ChangeObject],
    deletion_line_index: usize,
    addition_line_index: usize,
    bucket: &mut RenderBucket,
) {
    let mut deletion_offset = 0;
    let mut addition_offset = 0;
    let mut deletion_start: Option<usize> = None;
    let mut deletion_end = 0;
    let mut addition_start: Option<usize> = None;
    let mut addition_end = 0;
    for change in line_diff {
        let length = utf16::len(&change.value);
        if change.removed {
            deletion_start.get_or_insert(deletion_offset);
            deletion_offset += length;
            deletion_end = deletion_offset;
        } else if change.added {
            addition_start.get_or_insert(addition_offset);
            addition_offset += length;
            addition_end = addition_offset;
        } else {
            deletion_offset += length;
            addition_offset += length;
        }
    }
    if let Some(span_start) = deletion_start {
        bucket
            .deletion_decorations
            .push(create_diff_span_decoration(CreateDiffSpanDecorationProps {
                line: deletion_line_index,
                span_start,
                span_length: deletion_end - span_start,
            }));
    }
    if let Some(span_start) = addition_start {
        bucket
            .addition_decorations
            .push(create_diff_span_decoration(CreateDiffSpanDecorationProps {
                line: addition_line_index,
                span_start,
                span_length: addition_end - span_start,
            }));
    }
}

fn compute_line_diff_decorations(
    deletion_line: &str,
    addition_line: &str,
    bucket: &mut RenderBucket,
    line_diff_type: LineDiffTypes,
    max_line_diff_length: usize,
) {
    if line_diff_type == LineDiffTypes::None {
        return;
    }
    let deletion_line = clean_last_newline(deletion_line);
    let addition_line = clean_last_newline(addition_line);
    // If we have really long lines, we probably shouldn't compute diffs on them.
    if utf16::len(deletion_line) > max_line_diff_length
        || utf16::len(addition_line) > max_line_diff_length
    {
        return;
    }
    let deletion_line_index = bucket.deletion_content.length;
    let addition_line_index = bucket.addition_content.length;
    // NOTE(amadeus): Because we visually trim trailing newlines when rendering,
    // we also gotta make sure the diff parsing doesn't include the newline
    // character that could be there...
    let line_diff = if line_diff_type == LineDiffTypes::Char {
        diff_chars(deletion_line, addition_line)
    } else {
        diff_words_with_space(deletion_line, addition_line)
    };
    if line_diff_type == LineDiffTypes::WordLine {
        push_word_line_decorations(&line_diff, deletion_line_index, addition_line_index, bucket);
        return;
    }
    let mut deletion_spans: Vec<Span> = Vec::new();
    let mut addition_spans: Vec<Span> = Vec::new();
    let enable_join = line_diff_type == LineDiffTypes::WordAlt;
    let last_index = line_diff.len().wrapping_sub(1);
    for (index, item) in line_diff.iter().enumerate() {
        let is_last_item = index == last_index;
        if !item.added && !item.removed {
            for spans in [&mut deletion_spans, &mut addition_spans] {
                push_or_join_span(
                    spans,
                    PushOrJoinSpanProps {
                        item,
                        enable_join,
                        is_neutral: true,
                        is_last_item,
                    },
                );
            }
        } else if item.removed {
            push_or_join_span(
                &mut deletion_spans,
                PushOrJoinSpanProps {
                    item,
                    enable_join,
                    is_neutral: false,
                    is_last_item,
                },
            );
        } else {
            push_or_join_span(
                &mut addition_spans,
                PushOrJoinSpanProps {
                    item,
                    enable_join,
                    is_neutral: false,
                    is_last_item,
                },
            );
        }
    }
    let mut span_index = 0;
    for span in &deletion_spans {
        let span_length = utf16::len(&span.text);
        if span.highlighted {
            bucket
                .deletion_decorations
                .push(create_diff_span_decoration(CreateDiffSpanDecorationProps {
                    line: deletion_line_index,
                    span_start: span_index,
                    span_length,
                }));
        }
        span_index += span_length;
    }
    span_index = 0;
    for span in &addition_spans {
        let span_length = utf16::len(&span.text);
        if span.highlighted {
            bucket
                .addition_decorations
                .push(create_diff_span_decoration(CreateDiffSpanDecorationProps {
                    line: addition_line_index,
                    span_start: span_index,
                    span_length,
                }));
        }
        span_index += span_length;
    }
}

#[derive(Debug, Clone, Copy)]
struct HighlightSegment {
    // Where to place the highlighted line in RenderDiffFilesResult
    target_index: usize,
    // The where the highlighted region starts
    original_offset: usize,
    // Number of highlighted lines
    count: usize,
}

/// Upstream's `FakeArrayType`: a string being built plus a line count.
#[derive(Debug, Default)]
struct FakeArray {
    value: String,
    length: usize,
}

impl FakeArray {
    fn push(&mut self, value: &str) {
        self.value.push_str(value);
        self.length += 1;
    }
}

#[derive(Debug, Default)]
struct RenderBucket {
    deletion_content: FakeArray,
    addition_content: FakeArray,
    deletion_info: Vec<Option<LineInfo>>,
    addition_info: Vec<Option<LineInfo>>,
    deletion_decorations: Vec<DecorationItem>,
    addition_decorations: Vec<DecorationItem>,
    deletion_segments: Vec<HighlightSegment>,
    addition_segments: Vec<HighlightSegment>,
}

type SideInput<'input> = (
    &'input str,
    &'input str,
    &'input [Option<LineInfo>],
    &'input [DecorationItem],
);

fn render_two_files(
    highlighter: &dyn DiffsHighlighter,
    options: &RenderDiffOptions,
    language_override: Option<&str>,
    deletion: SideInput<'_>,
    addition: SideInput<'_>,
) -> (Vec<ElementContent>, Vec<ElementContent>) {
    let render_file = |(name, contents, line_info, decorations): SideInput<'_>| {
        if contents.is_empty() {
            return Vec::new();
        }
        let lang =
            language_override.map_or_else(|| get_filetype_from_file_name(name), str::to_owned);
        let tokens: Vec<Vec<ThemedToken>> = highlighter.code_to_tokens(
            clean_last_newline(contents),
            &get_token_options(
                &lang,
                &options.theme,
                Some(options.tokenize_max_line_length),
            ),
        );
        render_token_lines(
            &tokens,
            RenderTokenLinesOptions {
                state: Some(line_info),
                merge_whitespaces: MergeWhitespaces::Always,
                decorations,
            },
        )
    };
    (render_file(deletion), render_file(addition))
}
