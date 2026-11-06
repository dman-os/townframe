//! Differential tests for S4: the published dist's `iterateOverDiff`,
//! `virtualDiffLayout`, `computeEstimatedDiffHeights`,
//! `createWindowFromScrollPosition`, `computeFileOffsets`, `trimPatchContext`
//! and `hydratePartialDiff` ran over parsed diffs (corpus, seeded synthetic
//! file pairs, partial diffs from patch text) and fuzzed options
//! (`oracle/golden/layout.json`). The diffs themselves are passed to both
//! implementations as JSON, so none of this depends on which diff algorithm
//! produced the hunks and every comparison is exact. Test-only, original to
//! this crate.

use serde::Deserialize;
use serde_json::Value;

use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::layout::VirtualFileMetrics;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::HunkExpansionRegion;
use crate::types::options::HunkSeparators;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParseDiffOptions;
use crate::utf16;
use crate::utils::compute_estimated_diff_heights::ComputeEstimatedDiffHeightsOptions;
use crate::utils::compute_estimated_diff_heights::compute_estimated_diff_heights;
use crate::utils::compute_file_offsets::compute_line_offsets;
use crate::utils::compute_file_offsets::count_line_breaks;
use crate::utils::compute_file_offsets::lines_from_file_contents;
use crate::utils::create_window_from_scroll_position::WindowFromScrollPositionProps;
use crate::utils::create_window_from_scroll_position::create_window_from_scroll_position;
use crate::utils::hydrate_partial_diff::hydrate_partial_diff_clone;
use crate::utils::hydrate_partial_diff::hydrate_partial_diff_merge;
use crate::utils::iterate_over_diff::DiffLineCallbackProps;
use crate::utils::iterate_over_diff::DiffLineMetadata;
use crate::utils::iterate_over_diff::DiffLineType;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;
use crate::utils::trim_patch_context::trim_patch_context;
use crate::utils::virtual_diff_layout::GetExpandedRegionProps;
use crate::utils::virtual_diff_layout::IsAdditionLineRenderableProps;
use crate::utils::virtual_diff_layout::RenderableDirection;
use crate::utils::virtual_diff_layout::get_expanded_region;
use crate::utils::virtual_diff_layout::get_nearest_renderable_addition_line;
use crate::utils::virtual_diff_layout::is_addition_line_renderable;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Golden {
    diffs: Vec<FileDiffMetadata>,
    iterate: Vec<IterateCase>,
    expanded_regions: Vec<ExpandedRegionCase>,
    renderable: Vec<RenderableCase>,
    heights: Vec<HeightsCase>,
    windows: Vec<WindowCase>,
    line_offsets: Vec<OffsetsCase>,
    trims: Vec<TrimCase>,
    hydrations: Vec<HydrationCase>,
}

fn golden() -> Golden {
    load_golden("layout.json")
}

/// `null | true | [[hunkIndex, {fromStart, fromEnd}]]`
fn decode_expanded(value: &Value) -> Option<ExpandedHunks> {
    match value {
        Value::Null => None,
        Value::Bool(true) => Some(ExpandedHunks::All),
        Value::Array(entries) => Some(ExpandedHunks::Regions(
            entries
                .iter()
                .map(|entry| {
                    let index = entry[0].as_u64().expect("index") as usize;
                    let region: HunkExpansionRegion =
                        serde_json::from_value(entry[1].clone()).expect("region");
                    (index, region)
                })
                .collect(),
        )),
        other => panic!("unexpected expanded encoding {other}"),
    }
}

type Line = (usize, usize, usize, usize, bool);
type Row = (String, usize, usize, usize, Option<Line>, Option<Line>);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IterateCase {
    diff: usize,
    diff_style: String,
    starting_line: usize,
    total_lines: Option<usize>,
    threshold: Option<usize>,
    expanded: Value,
    rows: Vec<Row>,
}

fn line_tuple(line: Option<DiffLineMetadata>) -> Option<Line> {
    line.map(|line| {
        (
            line.unified_line_index,
            line.split_line_index,
            line.line_index,
            line.line_number,
            line.no_eofcr,
        )
    })
}

fn row_tuple(props: &DiffLineCallbackProps<'_>) -> Row {
    let line_type = match props.line_type {
        DiffLineType::Change => "change",
        DiffLineType::Context => "context",
        DiffLineType::ContextExpanded => "context-expanded",
    };
    (
        line_type.to_owned(),
        props.hunk_index,
        props.collapsed_before,
        props.collapsed_after,
        line_tuple(props.deletion_line),
        line_tuple(props.addition_line),
    )
}

#[test]
fn iterate_over_diff_matches_the_published_iterator() {
    let golden = golden();
    assert!(golden.iterate.len() > 80);
    let mut windowed = 0;
    for case in &golden.iterate {
        let diff = &golden.diffs[case.diff];
        let expanded = decode_expanded(&case.expanded);
        let diff_style = match case.diff_style.as_str() {
            "unified" => IterationDiffStyle::Unified,
            "split" => IterationDiffStyle::Split,
            "both" => IterationDiffStyle::Both,
            other => panic!("unknown style {other}"),
        };
        let mut rows: Vec<Row> = Vec::new();
        iterate_over_diff(
            IterateOverDiffProps {
                diff,
                diff_style,
                starting_line: case.starting_line,
                total_lines: case.total_lines,
                expanded_hunks: expanded.as_ref(),
                collapsed_context_threshold: case
                    .threshold
                    .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD),
            },
            |props| {
                rows.push(row_tuple(&props));
                false
            },
        );
        assert_eq!(
            rows,
            case.rows,
            "diff #{} style={} start={} total={:?} threshold={:?} expanded={}",
            case.diff,
            case.diff_style,
            case.starting_line,
            case.total_lines,
            case.threshold,
            case.expanded
        );
        windowed += usize::from(case.total_lines.is_some());
    }
    assert!(windowed > 20, "windowed cases: {windowed}");
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpandedRegionCase {
    is_partial: bool,
    range_size: i64,
    threshold: usize,
    expanded: Value,
    expected: ExpectedRegion,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedRegion {
    from_start: usize,
    from_end: usize,
    range_size: usize,
    collapsed_lines: usize,
    render_all: bool,
}

#[test]
fn get_expanded_region_matches() {
    let golden = golden();
    assert!(golden.expanded_regions.len() > 50);
    for case in &golden.expanded_regions {
        let expanded = decode_expanded(&case.expanded);
        let actual = get_expanded_region(GetExpandedRegionProps {
            is_partial: case.is_partial,
            range_size: usize::try_from(case.range_size.max(0)).expect("non-negative"),
            expanded_hunks: expanded.as_ref(),
            hunk_index: 2,
            collapsed_context_threshold: case.threshold,
        });
        assert_eq!(
            (
                actual.from_start,
                actual.from_end,
                actual.range_size,
                actual.collapsed_lines,
                actual.render_all
            ),
            (
                case.expected.from_start,
                case.expected.from_end,
                case.expected.range_size,
                case.expected.collapsed_lines,
                case.expected.render_all
            ),
            "isPartial={} rangeSize={} threshold={} expanded={}",
            case.is_partial,
            case.range_size,
            case.threshold,
            case.expanded
        );
    }
}

#[derive(Deserialize)]
struct RenderableCase {
    diff: usize,
    threshold: usize,
    expanded: Value,
    renderable: Vec<bool>,
    down: Vec<Option<usize>>,
    up: Vec<Option<usize>>,
}

#[test]
fn addition_line_renderability_matches() {
    let golden = golden();
    for case in &golden.renderable {
        let diff = &golden.diffs[case.diff];
        let expanded = decode_expanded(&case.expanded);
        for (line_number, expected) in case.renderable.iter().enumerate() {
            let props = IsAdditionLineRenderableProps {
                file_diff: diff,
                line_number,
                expanded_hunks: expanded.as_ref(),
                collapsed_context_threshold: case.threshold,
            };
            let context = format!(
                "diff #{} line {line_number} threshold {} expanded {}",
                case.diff, case.threshold, case.expanded
            );
            assert_eq!(is_addition_line_renderable(props), *expected, "{context}");
            assert_eq!(
                get_nearest_renderable_addition_line(props, RenderableDirection::Down),
                case.down[line_number],
                "down {context}"
            );
            assert_eq!(
                get_nearest_renderable_addition_line(props, RenderableDirection::Up),
                case.up[line_number],
                "up {context}"
            );
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeightsCase {
    diff: usize,
    metrics: VirtualFileMetrics,
    disable_file_header: bool,
    hunk_separators: HunkSeparators,
    expand_unchanged: bool,
    expanded: Value,
    threshold: usize,
    can_hydrate_partial_diff: bool,
    expected: ExpectedHeights,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedHeights {
    split_height: f64,
    unified_height: f64,
}

#[test]
fn estimated_diff_heights_match() {
    let golden = golden();
    assert!(golden.heights.len() > 50);
    for case in &golden.heights {
        let expanded = decode_expanded(&case.expanded);
        let actual = compute_estimated_diff_heights(ComputeEstimatedDiffHeightsOptions {
            file_diff: &golden.diffs[case.diff],
            metrics: &case.metrics,
            disable_file_header: case.disable_file_header,
            hunk_separators: case.hunk_separators,
            expand_unchanged: case.expand_unchanged,
            expanded_hunks: expanded.as_ref(),
            collapsed_context_threshold: case.threshold,
            can_hydrate_partial_diff: case.can_hydrate_partial_diff,
        });
        assert_eq!(
            (actual.split_height, actual.unified_height),
            (case.expected.split_height, case.expected.unified_height),
            "diff #{} metrics {:?} separators {:?}",
            case.diff,
            case.metrics,
            case.hunk_separators
        );
    }
}

#[derive(Deserialize)]
struct WindowCase {
    input: WindowInput,
    expected: ExpectedWindow,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WindowInput {
    scroll_top: f64,
    height: f64,
    scroll_height: f64,
    fit_perfectly: Option<bool>,
    fit_perfectly_overscroll: Option<f64>,
    overscroll_size: f64,
}

#[derive(Deserialize)]
struct ExpectedWindow {
    top: f64,
    bottom: f64,
}

#[test]
fn window_from_scroll_position_matches() {
    let golden = golden();
    assert!(golden.windows.len() > 50);
    for case in &golden.windows {
        let input = &case.input;
        let actual = create_window_from_scroll_position(WindowFromScrollPositionProps {
            scroll_top: input.scroll_top,
            height: input.height,
            scroll_height: input.scroll_height,
            fit_perfectly: input.fit_perfectly.unwrap_or(false),
            fit_perfectly_overscroll: input.fit_perfectly_overscroll.unwrap_or(0.0),
            overscroll_size: input.overscroll_size,
        });
        assert_eq!(
            (actual.top, actual.bottom),
            (case.expected.top, case.expected.bottom),
            "{:?}",
            (
                input.scroll_top,
                input.height,
                input.scroll_height,
                input.fit_perfectly,
                input.fit_perfectly_overscroll,
                input.overscroll_size
            )
        );
    }
}

#[derive(Deserialize)]
struct OffsetsCase {
    text: String,
    offsets: Vec<usize>,
    breaks: usize,
    lines: Vec<String>,
}

#[test]
fn line_offsets_match_in_utf16_units() {
    let golden = golden();
    for case in &golden.line_offsets {
        // JavaScript offsets are UTF-16 indexes; ours are byte offsets.
        let in_utf16: Vec<usize> = compute_line_offsets(&case.text)
            .into_iter()
            .map(|offset| utf16::utf16_offset(&case.text, offset))
            .collect();
        assert_eq!(in_utf16, case.offsets, "{:?}", case.text);
        assert_eq!(
            count_line_breaks(&case.text),
            case.breaks,
            "{:?}",
            case.text
        );
        assert_eq!(
            lines_from_file_contents(&case.text),
            case.lines,
            "{:?}",
            case.text
        );
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrimCase {
    input: String,
    context_size: usize,
    expected: String,
}

#[test]
fn trim_patch_context_matches() {
    let golden = golden();
    assert!(golden.trims.len() > 20);
    for case in &golden.trims {
        assert_eq!(
            trim_patch_context(&case.input, case.context_size),
            case.expected,
            "contextSize={} input={:?}",
            case.context_size,
            case.input
        );
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HydrationCase {
    kind: String,
    partial: FileDiffMetadata,
    files: LoadedFiles,
    options: HydrationOptions,
    expected: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadedFiles {
    old_file: Option<FileContents>,
    new_file: FileContents,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct HydrationOptions {
    disable_line_matching: Option<bool>,
}

#[test]
fn hydrate_partial_diff_matches() {
    let golden = golden();
    assert!(golden.hydrations.len() > 8);
    for case in &golden.hydrations {
        let files = match &case.files.old_file {
            Some(old_file) => FileDiffLoadedFiles::Changed {
                old_file: old_file.clone(),
                new_file: case.files.new_file.clone(),
            },
            None => FileDiffLoadedFiles::PureRenamed {
                new_file: case.files.new_file.clone(),
            },
        };
        let options = ParseDiffOptions {
            disable_line_matching: case.options.disable_line_matching,
            ..ParseDiffOptions::default()
        };
        let actual = match case.kind.as_str() {
            "clone" => {
                let before = case.partial.clone();
                let hydrated = hydrate_partial_diff_clone(&case.partial, &files, &options);
                assert_eq!(case.partial, before, "clone must not mutate its input");
                hydrated
            }
            "merge" => {
                let mut merged = case.partial.clone();
                hydrate_partial_diff_merge(&mut merged, &files, &options);
                merged
            }
            other => panic!("unknown kind {other}"),
        };
        assert_json_eq(
            &format!("{} hydration of {}", case.kind, case.partial.name),
            &case.expected,
            &serde_json::to_value(&actual).expect("serializes"),
        );
    }
}
