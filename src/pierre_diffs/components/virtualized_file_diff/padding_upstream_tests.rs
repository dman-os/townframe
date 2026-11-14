//! The `VirtualizedFileDiff` cases of `test/virtualFileMetricsPadding.test.ts`
//! (the `getLinePosition` ones) and `test/sparseLayoutCheckpoints.test.ts`, ported
//! with their names over the harness of `window_upstream_tests.rs` (see its header
//! for the conversions). Test-only.

use std::rc::Rc;

use super::layout_upstream_tests::METRICS;
use super::layout_upstream_tests::create_two_hunk_diff;
use super::layout_upstream_tests::padding_base_metrics;
use super::layout_upstream_tests::parse_files;
use super::layout_upstream_tests::plain_file;
use super::window_upstream_tests::DIFF_HEADER_HEIGHT;
use super::window_upstream_tests::LINE_HEIGHT;
use super::window_upstream_tests::Options;
use super::window_upstream_tests::SPACING;
use super::window_upstream_tests::World;
use super::window_upstream_tests::world;
use crate::components::file_diff::FileDiffHunkSeparators;
use crate::js_collections::PtrEq;
use crate::types::patch::FileDiffMetadata;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;

// ---- virtualFileMetricsPadding.test.ts, describe('VirtualizedFileDiff') ----

/// `createVirtualizedFileDiff(metrics, options)`
fn padding_world(metrics: PartialVirtualFileMetrics, disable_file_header: bool) -> World {
    let file_diff = parse_files(
        plain_file("file.ts", "one\ntwo\nthree"),
        plain_file("file.ts", "one\nTWO\nthree"),
    );
    let mut options = Options::default();
    options.base.base.disable_file_header = disable_file_header.then_some(true);
    let world = world(options, PartialVirtualFileMetrics { ..metrics });
    world.layout(&file_diff, None);
    world
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_uses_the_header_height_as_the_default_header_enabled_top_region()
 {
    let world = padding_world(padding_base_metrics(), false);
    assert_eq!(world.position(1).map(|p| p.0), Some(DIFF_HEADER_HEIGHT));
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_applies_padding_top_after_the_header() {
    let world = padding_world(
        PartialVirtualFileMetrics {
            padding_top: Some(6.0),
            ..padding_base_metrics()
        },
        false,
    );
    assert_eq!(
        world.position(1).map(|p| p.0),
        Some(DIFF_HEADER_HEIGHT + 6.0)
    );
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_uses_spacing_as_the_default_header_disabled_top_region()
 {
    let world = padding_world(padding_base_metrics(), true);
    assert_eq!(world.position(1).map(|p| p.0), Some(SPACING));
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_allows_padding_top_to_remove_the_header_disabled_top_region()
 {
    let world = padding_world(
        PartialVirtualFileMetrics {
            padding_top: Some(0.0),
            ..padding_base_metrics()
        },
        true,
    );
    assert_eq!(world.position(1).map(|p| p.0), Some(0.0));
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_uses_only_the_header_when_collapsed() {
    let file_diff = create_two_hunk_diff("base");
    let first_hunk_start = file_diff.hunks[0].addition_start;
    let mut options = Options::default();
    options.base.base.collapsed = Some(true);
    let world = world(
        options,
        PartialVirtualFileMetrics {
            padding_top: Some(6.0),
            padding_bottom: Some(13.0),
            ..padding_base_metrics()
        },
    );

    world.layout(&file_diff, None);

    assert_eq!(world.height(), DIFF_HEADER_HEIGHT);
    assert_eq!(
        world.position(first_hunk_start),
        Some((DIFF_HEADER_HEIGHT, 0.0))
    );
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_keeps_hunk_separator_gaps_based_on_spacing() {
    let numbered =
        |edit: &dyn Fn(usize) -> String| (0..20).map(edit).collect::<Vec<_>>().join("\n");
    let file_diff = parse_files(
        plain_file("file.ts", &numbered(&|index| (index + 1).to_string())),
        plain_file(
            "file.ts",
            &numbered(&|index| match index {
                1 => "two".to_owned(),
                18 => "nineteen".to_owned(),
                _ => (index + 1).to_string(),
            }),
        ),
    );
    let world = world(
        Options::default(),
        PartialVirtualFileMetrics {
            padding_top: Some(50.0),
            padding_bottom: Some(60.0),
            ..padding_base_metrics()
        },
    );
    world.layout(&file_diff, None);
    let (first, second) = (&file_diff.hunks[0], &file_diff.hunks[1]);

    assert_eq!(
        world.position(second.addition_start).map(|p| p.0),
        Some(
            DIFF_HEADER_HEIGHT
                + 50.0
                + first.split_line_count as f64 * LINE_HEIGHT
                + 12.0
                + SPACING * 2.0
        )
    );
}

/// `codeViewLikeMetrics`: `DEFAULT_CODE_VIEW_FILE_METRICS` with the test's overrides.
fn code_view_like_metrics() -> PartialVirtualFileMetrics {
    METRICS
}

fn two_hunk_world(options: Options) -> (World, Rc<FileDiffMetadata>) {
    let file_diff = create_two_hunk_diff("base");
    let world = world(options, code_view_like_metrics());
    world.layout(&file_diff, None);
    assert!(file_diff.hunks[0].collapsed_before > 0);
    assert!(file_diff.hunks[1].collapsed_before > 0);
    (world, file_diff)
}

fn separators(separators: FileDiffHunkSeparators<u8>) -> Options {
    Options {
        hunk_separators: Some(separators),
        ..Options::default()
    }
}

fn first_second_tops(world: &World, file_diff: &FileDiffMetadata) -> (Option<f64>, Option<f64>) {
    (
        world
            .position(file_diff.hunks[0].addition_start)
            .map(|p| p.0),
        world
            .position(file_diff.hunks[1].addition_start)
            .map(|p| p.0),
    )
}

fn line_info_checks(world: &World, file_diff: &FileDiffMetadata) {
    let [first, second] = [&file_diff.hunks[0], &file_diff.hunks[1]];
    let separator = 32.0;
    let first_separator = separator + SPACING;
    let middle = SPACING + separator + SPACING;
    let trailing = SPACING + separator;
    let hunk_lines = (first.split_line_count + second.split_line_count) as f64 * LINE_HEIGHT;
    assert_eq!(
        first_second_tops(world, file_diff),
        (
            Some(DIFF_HEADER_HEIGHT + first_separator),
            Some(
                DIFF_HEADER_HEIGHT
                    + first_separator
                    + first.split_line_count as f64 * LINE_HEIGHT
                    + middle
            )
        )
    );
    assert_eq!(
        world.height(),
        DIFF_HEADER_HEIGHT + first_separator + hunk_lines + middle + trailing + SPACING
    );
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_keeps_current_line_info_separator_estimates_for_first_middle_and_trailing_collapsed_context()
 {
    let (world, file_diff) = two_hunk_world(separators(FileDiffHunkSeparators::LineInfo));
    line_info_checks(&world, &file_diff);
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_keeps_current_custom_separator_estimates_aligned_with_line_info_gaps()
 {
    let custom = FileDiffHunkSeparators::Custom(PtrEq(Rc::new(|_: &_| None)));
    let (world, file_diff) = two_hunk_world(separators(custom));
    line_info_checks(&world, &file_diff);
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_keeps_current_line_info_basic_separator_estimates_without_spacing_gaps()
 {
    let (world, file_diff) = two_hunk_world(separators(FileDiffHunkSeparators::LineInfoBasic));
    let [first, second] = [&file_diff.hunks[0], &file_diff.hunks[1]];
    let separator = 32.0;
    let hunk_lines = (first.split_line_count + second.split_line_count) as f64 * LINE_HEIGHT;
    assert_eq!(
        first_second_tops(&world, &file_diff),
        (
            Some(DIFF_HEADER_HEIGHT + separator),
            Some(
                DIFF_HEADER_HEIGHT
                    + separator
                    + first.split_line_count as f64 * LINE_HEIGHT
                    + separator
            )
        )
    );
    assert_eq!(
        world.height(),
        DIFF_HEADER_HEIGHT + separator + hunk_lines + separator + separator + SPACING
    );
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_uses_built_in_simple_separator_measurements_with_code_view_metrics()
 {
    let (world, file_diff) = two_hunk_world(separators(FileDiffHunkSeparators::Simple));
    let [first, second] = [&file_diff.hunks[0], &file_diff.hunks[1]];
    assert_eq!(
        first_second_tops(&world, &file_diff),
        (
            Some(DIFF_HEADER_HEIGHT),
            Some(DIFF_HEADER_HEIGHT + first.split_line_count as f64 * LINE_HEIGHT + 4.0)
        )
    );
    assert_eq!(
        world.height(),
        DIFF_HEADER_HEIGHT
            + (first.split_line_count + second.split_line_count) as f64 * LINE_HEIGHT
            + 4.0
            + SPACING
    );
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_re_resolves_built_in_separator_measurements_when_options_change()
 {
    let (world, file_diff) = two_hunk_world(separators(FileDiffHunkSeparators::LineInfoBasic));
    let first = &file_diff.hunks[0];
    let second_start = file_diff.hunks[1].addition_start;
    assert_eq!(
        world.position(second_start).map(|p| p.0),
        Some(DIFF_HEADER_HEIGHT + 32.0 + first.split_line_count as f64 * LINE_HEIGHT + 32.0)
    );

    world.set_options(|options| options.hunk_separators = Some(FileDiffHunkSeparators::Simple));

    assert_eq!(
        world.position(second_start).map(|p| p.0),
        Some(DIFF_HEADER_HEIGHT + first.split_line_count as f64 * LINE_HEIGHT + 4.0)
    );
}

#[test]
fn virtual_file_padding_metrics_virtualized_file_diff_does_not_reserve_metadata_separator_height_for_final_collapsed_context()
 {
    let (world, file_diff) = two_hunk_world(separators(FileDiffHunkSeparators::Metadata));
    let [first, second] = [&file_diff.hunks[0], &file_diff.hunks[1]];
    assert_eq!(
        world.position(first.addition_start).map(|p| p.0),
        Some(DIFF_HEADER_HEIGHT + 32.0)
    );
    assert_eq!(
        world.height(),
        DIFF_HEADER_HEIGHT
            + 32.0 * 2.0
            + (first.split_line_count + second.split_line_count) as f64 * LINE_HEIGHT
            + SPACING
    );
}

// ---- sparseLayoutCheckpoints.test.ts ----

#[test]
fn sparse_layout_checkpoints_virtualized_file_diff_uses_checkpoints_for_deep_expanded_line_positions_and_anchors()
 {
    // `getNumericScrollAnchor` is dropped (only CodeView reads it): the
    // `getLinePosition` half.
    let mut options = Options::default();
    options.base.expand_unchanged = Some(true);
    let world = world(
        options,
        PartialVirtualFileMetrics {
            hunk_line_count: Some(1),
            hunk_separator_height: Some(12.0),
            ..METRICS
        },
    );
    let old_lines: Vec<String> = (1..=12_000).map(|index| index.to_string()).collect();
    let old_contents = old_lines.join("\n");
    let new_contents = old_contents.replace("\n6000\n", "\nchanged-6000\n");
    let file_diff = parse_files(
        plain_file("large.txt", &old_contents),
        plain_file("large.txt", &new_contents),
    );
    world.layout(&file_diff, None);

    assert_eq!(
        world.position(10_000).map(|p| p.0),
        Some(DIFF_HEADER_HEIGHT + 9_999.0 * LINE_HEIGHT)
    );
}

#[test]
fn sparse_layout_checkpoints_virtualized_file_diff_maps_hidden_collapsed_line_indexes_to_their_separator_row()
 {
    let numbered =
        |edit: &dyn Fn(usize) -> String| (0..120).map(edit).collect::<Vec<_>>().join("\n");
    let file_diff = parse_files(
        plain_file("collapsed.txt", &numbered(&|index| (index + 1).to_string())),
        plain_file(
            "collapsed.txt",
            &numbered(&|index| match index {
                1 => "changed-2".to_owned(),
                109 => "changed-110".to_owned(),
                _ => (index + 1).to_string(),
            }),
        ),
    );
    let world = world(
        Options::default(),
        PartialVirtualFileMetrics {
            hunk_line_count: Some(1),
            hunk_separator_height: Some(12.0),
            ..METRICS
        },
    );
    world.layout(&file_diff, None);
    let [first, second] = [&file_diff.hunks[0], &file_diff.hunks[1]];

    assert_eq!(
        world.position(second.addition_start - 2),
        Some((
            DIFF_HEADER_HEIGHT + first.split_line_count as f64 * LINE_HEIGHT + SPACING,
            12.0
        ))
    );
}
