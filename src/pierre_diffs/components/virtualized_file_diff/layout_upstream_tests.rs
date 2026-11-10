//! Upstream's `test/virtualizedFileDiffEstimatedHeights.test.ts`
//! (`describe('VirtualizedFileDiff estimated height cache')`) and
//! `test/virtualFileMetricsPadding.test.ts` (`describe('VirtualizedFileDiff')`)
//! cases that need only the layout model, ported with their names over
//! `LayoutInstance`.
//! `instance.updateCodeViewLayout(diff, 0)` (the CodeView layout pass, not
//! ported) is `LayoutInstance::update_layout`, its layout half. The cases that
//! need `getLinePosition`, `reconcileHeights`, `computeRenderRangeFromWindow`
//! (S12b-2), `render`/`expandHunk` on the instance (S12b-3) or CodeView's
//! `getAdvancedStickySpecs` are not here (see parity/divergences.psv).
//! Test-only.

use std::rc::Rc;

use serde_json::json;

use super::layout_test_instance::LayoutInstance;
use super::layout_test_instance::Options;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use crate::utils::parse_diff_from_file::parse_diff_from_file;

// `{ ...DEFAULT_CODE_VIEW_FILE_METRICS, hunkLineCount: 2, lineHeight: 10,
// diffHeaderHeight: 30, spacing: 4 }`
const METRICS: PartialVirtualFileMetrics = PartialVirtualFileMetrics {
    hunk_line_count: Some(2),
    line_height: Some(10.0),
    diff_header_height: Some(30.0),
    hunk_separator_height: None,
    spacing: Some(4.0),
    padding_top: None,
    padding_bottom: None,
};
const LINE_HEIGHT: f64 = 10.0;
const DIFF_HEADER_HEIGHT: f64 = 30.0;
const SPACING: f64 = 4.0;
const LINE_INFO_TRAILING_SEPARATOR_HEIGHT: f64 = SPACING + 32.0;

fn instance(options: Options) -> LayoutInstance {
    LayoutInstance::new(options, METRICS)
}

fn loader_options() -> Options {
    Options {
        has_diff_file_loader: true,
        ..Options::default()
    }
}

fn contents(name: &str, contents: String, cache_key: String) -> FileContents {
    FileContents {
        name: name.to_owned(),
        contents,
        lang: None,
        header: None,
        cache_key: Some(cache_key),
    }
}

fn create_two_hunk_diff(cache_key: &str) -> Rc<FileDiffMetadata> {
    let old_lines: Vec<String> = (0..140).map(|index| (index + 1).to_string()).collect();
    let new_lines: Vec<String> = old_lines
        .iter()
        .enumerate()
        .map(|(index, line)| match index {
            39 => format!("{cache_key}-changed-40"),
            99 => format!("{cache_key}-changed-100"),
            _ => line.clone(),
        })
        .collect();
    Rc::new(
        parse_diff_from_file(
            &DiffFileInput::Both {
                old_file: contents(
                    "two-hunks.ts",
                    format!("{}\n", old_lines.join("\n")),
                    format!("{cache_key}:old"),
                ),
                new_file: contents(
                    "two-hunks.ts",
                    format!("{}\n", new_lines.join("\n")),
                    format!("{cache_key}:new"),
                ),
            },
            &Default::default(),
            false,
        )
        .expect("the files diff")
        .value,
    )
}

/// `createHugeSingleBlockDiff`
fn create_huge_single_block_diff(line_count: usize) -> Rc<FileDiffMetadata> {
    Rc::new(
        serde_json::from_value(json!({
            "name": "huge.ts",
            "type": "change",
            "hunks": [{
                "collapsedBefore": 0,
                "additionStart": 1,
                "additionCount": line_count,
                "additionLines": 0,
                "additionLineIndex": 0,
                "deletionStart": 1,
                "deletionCount": line_count,
                "deletionLines": 0,
                "deletionLineIndex": 0,
                "hunkContent": [{
                    "type": "context",
                    "lines": line_count,
                    "additionLineIndex": 0,
                    "deletionLineIndex": 0,
                }],
                "splitLineStart": 0,
                "splitLineCount": line_count,
                "unifiedLineStart": 0,
                "unifiedLineCount": line_count,
                "noEOFCRDeletions": false,
                "noEOFCRAdditions": false,
            }],
            "splitLineCount": line_count,
            "unifiedLineCount": line_count,
            "isPartial": true,
            "deletionLines": [],
            "additionLines": [],
            "deletionCharacterCount": 0,
            "additionCharacterCount": 0,
        }))
        .expect("upstream's huge single block diff"),
    )
}

#[test]
fn estimated_height_cache_computes_split_and_unified_estimates_together_on_first_prepare() {
    let mut instance = instance(Options::default());

    instance.update_layout(&create_two_hunk_diff("base"), 0.0);

    assert_eq!(instance.layout.cache.estimated_split_height, Some(326.0));
    assert_eq!(instance.layout.cache.estimated_unified_height, Some(346.0));
    assert_eq!(instance.layout.cache.measured_height_delta_total, 0.0);
    assert_eq!(instance.layout.cache.total_lines, 0);
    assert!(instance.layout.cache.checkpoints.is_empty());
    assert_eq!(instance.layout.height, 326.0);
}

#[test]
fn estimated_height_cache_reserves_synthetic_bottom_separator_height_for_hydratable_partial_diffs()
{
    let line_count = 8;
    let mut instance = instance(loader_options());

    instance.update_layout(&create_huge_single_block_diff(line_count), 0.0);

    assert_eq!(
        instance.layout.height,
        DIFF_HEADER_HEIGHT
            + line_count as f64 * LINE_HEIGHT
            + LINE_INFO_TRAILING_SEPARATOR_HEIGHT
            + SPACING
    );
}

#[test]
fn estimated_height_cache_recomputes_estimates_when_file_loader_availability_changes() {
    let line_count = 8;
    let mut instance = instance(Options::default());

    instance.update_layout(&create_huge_single_block_diff(line_count), 0.0);
    instance.layout.cache.estimated_split_height = Some(123.0);
    instance.layout.cache.estimated_unified_height = Some(456.0);
    instance.layout.cache.height_deltas.insert(0, 7.0);
    instance.layout.cache.measured_height_delta_total = 7.0;

    instance.set_options(loader_options());

    let expected = DIFF_HEADER_HEIGHT
        + line_count as f64 * LINE_HEIGHT
        + LINE_INFO_TRAILING_SEPARATOR_HEIGHT
        + SPACING;
    assert_eq!(instance.layout.cache.estimated_split_height, Some(expected));
    assert_eq!(
        instance.layout.cache.estimated_unified_height,
        Some(expected)
    );
    assert_eq!(instance.layout.cache.height_deltas.len(), 0);
    assert_eq!(instance.layout.cache.measured_height_delta_total, 0.0);
    assert_eq!(instance.layout.height, expected);
}

#[test]
fn estimated_height_cache_keeps_estimates_and_measurements_for_an_equivalent_diff_cache_key() {
    let file_diff = create_two_hunk_diff("same");
    // `{ ...fileDiff, hunks: [...fileDiff.hunks] }`: another object, the same cache key.
    let equivalent_file_diff = Rc::new((*file_diff).clone());
    let mut instance = instance(Options::default());

    instance.update_layout(&file_diff, 0.0);
    instance.layout.cache.estimated_split_height = Some(123.0);
    instance.layout.cache.estimated_unified_height = Some(456.0);
    instance.layout.cache.height_deltas.insert(0, 7.0);
    instance.layout.cache.measured_height_delta_total = 7.0;
    instance.update_layout(&equivalent_file_diff, 0.0);

    assert_eq!(instance.layout.cache.estimated_split_height, Some(123.0));
    assert_eq!(instance.layout.cache.estimated_unified_height, Some(456.0));
    assert_eq!(instance.layout.cache.height_deltas.get(&0), Some(&7.0));
    assert_eq!(instance.layout.cache.measured_height_delta_total, 7.0);
}

#[test]
fn estimated_height_cache_clears_estimates_and_measurements_for_changed_diff_content() {
    let mut instance = instance(Options::default());

    instance.update_layout(&create_two_hunk_diff("first"), 0.0);
    instance.layout.cache.estimated_split_height = Some(123.0);
    instance.layout.cache.estimated_unified_height = Some(456.0);
    instance.layout.cache.height_deltas.insert(0, 7.0);
    instance.layout.cache.measured_height_delta_total = 7.0;
    instance.update_layout(&create_two_hunk_diff("second"), 0.0);

    assert_eq!(instance.layout.cache.estimated_split_height, Some(326.0));
    assert_eq!(instance.layout.cache.estimated_unified_height, Some(346.0));
    assert_eq!(instance.layout.cache.height_deltas.len(), 0);
    assert_eq!(instance.layout.cache.measured_height_delta_total, 0.0);
}

// `baseMetrics`: `{ ...DEFAULT_VIRTUAL_FILE_METRICS, hunkLineCount: 2, lineHeight: 10,
// diffHeaderHeight: 30, hunkSeparatorHeight: 12, spacing: 4 }`
fn padding_base_metrics() -> PartialVirtualFileMetrics {
    PartialVirtualFileMetrics {
        hunk_separator_height: Some(12.0),
        ..METRICS
    }
}

fn plain_file(name: &str, contents: &str) -> FileContents {
    FileContents {
        name: name.to_owned(),
        contents: contents.to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    }
}

fn parse_files(old_file: FileContents, new_file: FileContents) -> Rc<FileDiffMetadata> {
    Rc::new(
        parse_diff_from_file(
            &DiffFileInput::Both { old_file, new_file },
            &Default::default(),
            false,
        )
        .expect("the files diff")
        .value,
    )
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_uses_padding_bottom_instead_of_spacing_in_total_height()
 {
    // `createVirtualizedFileDiff({ paddingBottom: 13 })`
    let file_diff = parse_files(
        plain_file("file.ts", "one\ntwo\nthree"),
        plain_file("file.ts", "one\nTWO\nthree"),
    );
    let mut instance = LayoutInstance::new(
        Options::default(),
        PartialVirtualFileMetrics {
            padding_bottom: Some(13.0),
            ..padding_base_metrics()
        },
    );
    instance.update_layout(&file_diff, 0.0);

    assert_eq!(
        instance.layout.height,
        DIFF_HEADER_HEIGHT + 4.0 * LINE_HEIGHT + 13.0
    );
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_does_not_add_padding_bottom_when_a_diff_has_no_hunks()
 {
    let file_diff = parse_files(
        plain_file("same.ts", "one\n"),
        plain_file("same.ts", "one\n"),
    );
    let mut instance = LayoutInstance::new(
        Options::default(),
        PartialVirtualFileMetrics {
            padding_top: Some(6.0),
            padding_bottom: Some(13.0),
            ..padding_base_metrics()
        },
    );

    instance.update_layout(&file_diff, 0.0);

    assert_eq!(file_diff.hunks.len(), 0);
    assert_eq!(instance.layout.height, DIFF_HEADER_HEIGHT + 6.0);
}
