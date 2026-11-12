// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/VirtualizedFileDiff.setVisibility.test.ts
// (and cases of test/virtualizedFileDiffEstimatedHeights.test.ts and
// test/sparseLayoutCheckpoints.test.ts, listed below).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case.

//! Upstream's `VirtualizedFileDiff` cases that need the integration with
//! `FileDiff` (S12b-3): `render`, `expandHunk`, `setOptions`' notification of the
//! virtualizer, `cleanUp(true)` and `setVisibility`. Same names (prefixed by
//! their file's `describe`, or the file); the worker pool the first one passes
//! (`createInitializedManager`) is dropped, the virtualizer the tests stub is the
//! real `Virtualizer` over `FakeDom`'s rule geometry (a container the rules put
//! far below the window is off-screen). Test-only.
//! * `virtualizedFileDiffEstimatedHeights.test.ts`: `retains file-level annotation
//!   height through recycling until the top remounts`, `ignores trailing fromEnd
//!   expansion in render range line totals`.
//! * `sparseLayoutCheckpoints.test.ts`: `VirtualizedFileDiff marks diff indicator
//!   changes as layout dirty` (the stub records `instanceChanged`'s second
//!   argument; the real virtualizer's `markDOMDirty` is what it does with a
//!   true one, read by `Virtualizer::is_dom_dirty`).
//! * `VirtualizedFileDiff.setVisibility.test.ts`: `renders a replacement after an
//!   off-screen placeholder`.

use std::rc::Rc;

use super::instance::VirtualizedFileDiff;
use super::layout::get_expanded_line_count;
use super::layout_upstream_tests::METRICS;
use super::layout_upstream_tests::create_two_hunk_diff;
use super::layout_upstream_tests::parse_files;
use super::window_upstream_tests::LINE_HEIGHT;
use super::window_upstream_tests::annotated_world;
use super::window_upstream_tests::measured_group;
use super::window_upstream_tests::world;
use crate::components::file_diff::CodeGroups;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::FileDiffRenderProps;
use crate::components::virtualizer::Root;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerConfig;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::ExpansionDirections;
use crate::types::lines::HunkExpansionRegion;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;

/// `countIteratedRows`
fn count_iterated_rows(
    file_diff: &FileDiffMetadata,
    diff_style: IterationDiffStyle,
    expanded_hunks: &ExpandedHunks<'_>,
) -> usize {
    let mut count = 0;
    iterate_over_diff(
        IterateOverDiffProps {
            expanded_hunks: Some(expanded_hunks),
            ..IterateOverDiffProps::new(file_diff, diff_style)
        },
        |_| {
            count += 1;
            false
        },
    );
    count
}

#[test]
fn estimated_height_cache_ignores_trailing_from_end_expansion_in_render_range_line_totals() {
    let world = world(FileDiffOptions::default(), METRICS);
    let file_diff = create_two_hunk_diff("base");
    let trailing_hunk_index = file_diff.hunks.len();
    let from_start_only = ExpandedHunks::owned(
        [(
            trailing_hunk_index,
            HunkExpansionRegion {
                from_start: 2,
                from_end: 0,
            },
        )]
        .into(),
    );
    let container = world.dom.doc.create_element("div");

    assert!(world.instance.render(FileDiffRenderProps {
        file_diff: Some(Rc::clone(&file_diff)),
        file_container: Some(container),
        defer_managers: true,
        prevent_emit: true,
        ..FileDiffRenderProps::default()
    }));
    world.instance.expand_hunk(
        trailing_hunk_index as i64,
        ExpansionDirections::Up,
        Some(2.0),
    );
    world.instance.expand_hunk(
        trailing_hunk_index as i64,
        ExpansionDirections::Down,
        Some(3.0),
    );

    for (style, iteration_style) in [
        (DiffStyle::Split, IterationDiffStyle::Split),
        (DiffStyle::Unified, IterationDiffStyle::Unified),
    ] {
        let counted = world.instance.with_layout(Some(&file_diff), |_, ctx| {
            get_expanded_line_count(&file_diff, style, ctx)
        });
        assert_eq!(
            counted,
            count_iterated_rows(&file_diff, iteration_style, &from_start_only)
        );
    }
    world.instance.clean_up(false);
}

#[test]
fn sparse_layout_checkpoints_virtualized_file_diff_marks_diff_indicator_changes_as_layout_dirty() {
    let mut options = FileDiffOptions::default();
    options.base.diff_indicators = Some(DiffIndicators::Bars);
    let world = world(options, METRICS);
    let old_file = FileContents {
        name: "indicators.txt".to_owned(),
        contents: "one\ntwo\nthree".to_owned(),
        lang: None,
        header: None,
        cache_key: None,
    };
    let new_file = FileContents {
        contents: "one\nchanged\nthree".to_owned(),
        ..old_file.clone()
    };
    let virtualizer = Rc::clone(world.instance.virtualizer());
    // Set the virtualizer up and let its pass clear the geometry flags, so a
    // flag set afterwards is `instanceChanged(this, true)`'s doing.
    virtualizer.setup(Root::Document, None);
    world.dom.frames.flush_frames(16, 0.0);
    assert!(!virtualizer.is_dom_dirty());

    world
        .instance
        .update_code_view_layout(&parse_files(old_file, new_file), 0.0, None);
    world.set_options(|options| options.base.disable_background = Some(true));
    assert!(!virtualizer.is_dom_dirty());
    world.set_options(|options| options.base.diff_indicators = Some(DiffIndicators::Classic));
    assert!(virtualizer.is_dom_dirty());
}

#[test]
fn estimated_height_cache_retains_file_level_annotation_height_through_recycling_until_the_top_remounts()
 {
    let (world, file_diff, _) = annotated_world();
    assert!(world.instance.reconcile_heights());
    assert_eq!(world.height(), 351.0);
    world.cache(|cache| {
        assert_eq!(cache.file_annotation_height, 25.0);
        assert_eq!(cache.measured_height_delta_total, 25.0);
    });

    // `inspect(instance).fileContainer = undefined`
    world
        .instance
        .file_diff()
        .virtualized()
        .install_rendered_for_test(&file_diff, None, CodeGroups::default(), None);
    world.instance.clean_up(true);

    let none: crate::components::file_diff::LineAnnotations<u8> = Rc::new(Vec::new());
    world.layout(&file_diff, Some(&none));

    world.cache(|cache| {
        assert_eq!(cache.file_annotation_height, 25.0);
        assert_eq!(cache.measured_height_delta_total, 25.0);
    });
    assert_eq!(world.instance.file_diff().get_latest_annotations().len(), 0);
    assert_eq!(world.instance.file_diff().line_annotations().len(), 1);
    assert_eq!(world.height(), 351.0);

    // `setRenderedDiff`, a container, a render range and an unannotated group.
    let measured = measured_group(&world.dom, "0,0", LINE_HEIGHT, None, None);
    world.install(&file_diff, Some(&measured));
    world.render_range(0.0);
    assert!(world.instance.reconcile_heights());
    world.cache(|cache| {
        assert_eq!(cache.file_annotation_height, 0.0);
        assert_eq!(cache.measured_height_delta_total, 0.0);
    });
    assert_eq!(world.instance.file_diff().line_annotations().len(), 0);
    assert_eq!(world.height(), 326.0);
}

fn visibility_diff(cache_key: &str, contents: &str) -> Rc<FileDiffMetadata> {
    let file = |name_contents: String, key: &str| FileContents {
        name: "visibility.txt".to_owned(),
        contents: name_contents,
        lang: None,
        header: None,
        cache_key: Some(format!("{cache_key}:{key}")),
    };
    parse_files(
        file("before\n".to_owned(), "old"),
        file(format!("{contents}\n"), "new"),
    )
}

#[test]
fn virtualized_file_diff_set_visibility_renders_a_replacement_after_an_off_screen_placeholder() {
    let dom = Rc::new(FakeDom::default());
    {
        // The container sits far below the 1000px window and its margin.
        let mut world = dom.world.borrow_mut();
        world.rules = true;
        world.viewport_height = 1_000.0;
        world.container_top = 100_000.0;
    }
    let instance: Rc<VirtualizedFileDiff<FakeDom, u8, ()>> = VirtualizedFileDiff::new(
        Rc::clone(&dom),
        ResizeObserverHub::new(Rc::clone(&dom)),
        UniversalRenderingManager::new(Rc::clone(&dom)),
        Rc::new(PlainTextHighlighter),
        FileDiffOptions::default(),
        Virtualizer::new(
            Rc::clone(&dom),
            UniversalRenderingManager::new(Rc::clone(&dom)),
            VirtualizerConfig::default(),
        ),
        Default::default(),
        Some(Rc::new(|| ())),
    );
    let container = dom.doc.create_element("diffs-container");
    let first_diff = visibility_diff("first", "first value");
    let replacement_diff = visibility_diff("replacement", "replacement value");
    let placeholders = || {
        let shadow_root = dom.doc.shadow_root(container).expect("a shadow root");
        dom.doc
            .children(shadow_root)
            .into_iter()
            .filter(|child| dom.doc.has_attribute(*child, "data-placeholder"))
            .count()
    };

    instance.render(FileDiffRenderProps {
        file_diff: Some(Rc::clone(&first_diff)),
        file_container: Some(container),
        ..FileDiffRenderProps::default()
    });
    assert_eq!(placeholders(), 1);
    assert!(instance.file_diff().get_rendered_diff().is_none());

    instance.render(FileDiffRenderProps {
        file_diff: Some(Rc::clone(&replacement_diff)),
        file_container: Some(container),
        ..FileDiffRenderProps::default()
    });
    assert_eq!(placeholders(), 0);
    assert!(Rc::ptr_eq(
        &instance
            .file_diff()
            .get_rendered_diff()
            .expect("a rendered diff"),
        &replacement_diff
    ));
    instance.clean_up(false);
}
