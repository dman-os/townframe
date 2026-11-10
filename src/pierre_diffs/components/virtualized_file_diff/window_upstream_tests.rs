//! Upstream's `VirtualizedFileDiff` cases of `test/virtualizedFileDiffEstimatedHeights.test.ts`,
//! `test/virtualFileMetricsPadding.test.ts` and `test/sparseLayoutCheckpoints.test.ts`
//! that need the window and measurement half (S12b-2): `getLinePosition`,
//! `reconcileHeights` over declared rows, `computeRenderRangeFromWindow`. Same
//! names (prefixed by their `describe`); `updateCodeViewLayout(diff, 0, undefined,
//! annotations)` is `update_code_view_layout`. The upstream tests write the
//! private fields a render would (`renderedData`, `fileContainer`,
//! `codeAdditions`, `renderRange`) and fake `HTMLElement`s with a height
//! callback; here `install_rendered_for_test` writes them and the elements are
//! `FakeDom` nodes whose declared height the test changes. `setOptions` and
//! `expandHunk` are the instance's own (`overrides.rs`). The cases that need
//! `cleanUp(true)`, `render` or `instanceChanged` are in
//! `integration_upstream_tests.rs`. Not converted: `recomputeDiffHunks`
//! (`render range clamp reaches additionLines.length after stale layout cache`),
//! `getAdvancedStickySpecs` and `getNumericScrollAnchor` (dropped); see
//! parity/rules.psv. Test-only.

use std::rc::Rc;

use super::instance::VirtualizedFileDiff;
use super::layout::VIRTUALIZED_FILE_DIFF_LAYOUT_CHECKPOINT_INTERVAL;
use super::layout_upstream_tests::METRICS;
use super::layout_upstream_tests::create_huge_single_block_diff;
use super::layout_upstream_tests::create_two_hunk_diff;
use super::layout_upstream_tests::parse_files;
use super::layout_upstream_tests::plain_file;
use crate::components::file_diff::CodeGroups;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::LineAnnotations;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerConfig;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::layout::RenderRange;
use crate::types::layout::RenderWindow;
use crate::types::lines::ExpansionDirections;
use crate::types::options::DiffStyle;
use crate::types::options::Overflow;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectionSide;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;

type Instance = VirtualizedFileDiff<FakeDom, u8, u8>;
pub(super) type Options = FileDiffOptions<NodeKey, u8, u8>;

pub(super) const LINE_HEIGHT: f64 = 10.0;
pub(super) const DIFF_HEADER_HEIGHT: f64 = 30.0;
pub(super) const SPACING: f64 = 4.0;
const HUNK_LINE_COUNT: f64 = 2.0;

pub(super) struct World {
    pub(super) dom: Rc<FakeDom>,
    pub(super) instance: Rc<Instance>,
}

pub(super) fn world(options: Options, metrics: PartialVirtualFileMetrics) -> World {
    let dom = Rc::new(FakeDom::default());
    let virtualizer = Virtualizer::new(
        Rc::clone(&dom),
        UniversalRenderingManager::new(Rc::clone(&dom)),
        VirtualizerConfig::default(),
    );
    let instance = VirtualizedFileDiff::new(
        Rc::clone(&dom),
        ResizeObserverHub::new(Rc::clone(&dom)),
        UniversalRenderingManager::new(Rc::clone(&dom)),
        Rc::new(PlainTextHighlighter),
        options,
        virtualizer,
        metrics,
        None,
    );
    World { dom, instance }
}

fn annotations(entries: &[(AnnotationSide, usize)]) -> LineAnnotations<u8> {
    Rc::new(
        entries
            .iter()
            .map(|&(side, line_number)| DiffLineAnnotation {
                side,
                line_number,
                metadata: None,
            })
            .collect(),
    )
}

fn file_annotation() -> LineAnnotations<u8> {
    annotations(&[(AnnotationSide::Additions, 0)])
}

impl World {
    pub(super) fn layout(
        &self,
        file_diff: &Rc<FileDiffMetadata>,
        annotations: Option<&LineAnnotations<u8>>,
    ) {
        self.instance
            .update_code_view_layout(file_diff, 0.0, annotations);
    }

    pub(super) fn height(&self) -> f64 {
        self.instance.get_virtualized_height()
    }

    pub(super) fn cache<Output>(
        &self,
        read: impl FnOnce(&super::layout::DiffLayoutCache) -> Output,
    ) -> Output {
        read(&self.instance.layout().cache)
    }

    /// `VirtualizedFileDiff.setOptions` for a change of one option.
    pub(super) fn set_options(&self, change: impl FnOnce(&mut Options)) {
        let mut next = self.instance.file_diff().options().clone();
        change(&mut next);
        self.instance.set_options(Some(next));
    }

    /// `expandHunk` (the simple mode's).
    pub(super) fn expand_hunk(
        &self,
        hunk_index: usize,
        direction: ExpansionDirections,
        count: usize,
    ) {
        self.instance
            .expand_hunk(hunk_index as i64, direction, Some(count as f64));
    }

    fn set_cache_for_annotation(&self, annotation_height: f64) {
        self.instance.with_layout(None, |layout, _| {
            layout.cache.file_annotation_height = annotation_height;
            layout.cache.measured_height_delta_total = annotation_height;
            layout.height = 326.0 + annotation_height;
        });
    }

    fn window(&self, file_diff: &FileDiffMetadata, top: f64, bottom: f64) -> RenderRange {
        self.instance
            .compute_render_range_from_window(file_diff, 0.0, RenderWindow { top, bottom })
    }

    pub(super) fn position(&self, line: usize) -> Option<(f64, f64)> {
        self.instance
            .get_line_position(line, SelectionSide::Additions)
            .map(|position| (position.top, position.height))
    }
}

/// `createMeasuredCodeGroup*`: `group[gutter, content[annotations..., line]]`.
pub(super) struct Measured {
    group: NodeKey,
    line: NodeKey,
    file_annotation: Option<NodeKey>,
}

pub(super) fn measured_group(
    dom: &FakeDom,
    line_index: &str,
    line_height: f64,
    file_annotation_height: Option<f64>,
    first_row_annotation_height: Option<f64>,
) -> Measured {
    let doc = &dom.doc;
    let group = doc.create_element("div");
    let gutter = doc.create_element("div");
    let content = doc.create_element("div");
    doc.append_child(group, gutter);
    doc.append_child(group, content);
    let set_height = |node: NodeKey, height: f64| {
        dom.heights.borrow_mut().insert(node, height);
    };
    let mut file_annotation = None;
    if let Some(height) = file_annotation_height {
        let node = doc.create_element("div");
        doc.set_attribute(node, "data-line-annotation", "-1,-1");
        set_height(node, height);
        doc.append_child(content, node);
        file_annotation = Some(node);
    }
    if let Some(height) = first_row_annotation_height {
        let node = doc.create_element("div");
        doc.set_attribute(node, "data-line-annotation", "0,0");
        set_height(node, height);
        doc.append_child(content, node);
    }
    let line = doc.create_element("div");
    doc.set_attribute(line, "data-line-index", line_index);
    set_height(line, line_height);
    doc.append_child(content, line);
    Measured {
        group,
        line,
        file_annotation,
    }
}

impl World {
    /// `setRenderedDiff` + `fileContainer` + `codeAdditions`.
    pub(super) fn install(&self, file_diff: &Rc<FileDiffMetadata>, additions: Option<&Measured>) {
        let container = self.dom.doc.create_element("div");
        self.instance
            .file_diff()
            .virtualized()
            .install_rendered_for_test(
                file_diff,
                Some(container),
                CodeGroups {
                    additions: additions.map(|measured| measured.group),
                    ..CodeGroups::default()
                },
                None,
            );
    }

    /// Swap the rendered code group without touching the rest (`codeAdditions = ...`).
    fn set_additions(&self, file_diff: &Rc<FileDiffMetadata>, additions: &Measured) {
        let container = self
            .instance
            .file_diff()
            .virtualized()
            .file_container()
            .expect("a container");
        self.instance
            .file_diff()
            .virtualized()
            .install_rendered_for_test(
                file_diff,
                Some(container),
                CodeGroups {
                    additions: Some(additions.group),
                    ..CodeGroups::default()
                },
                None,
            );
    }

    pub(super) fn render_range(&self, starting_line: f64) {
        self.instance
            .file_diff()
            .virtualized()
            .set_render_range(Some(RenderRange {
                starting_line,
                total_lines: HUNK_LINE_COUNT,
                buffer_before: 0.0,
                buffer_after: 0.0,
            }));
    }
}

fn estimates(world: &World) -> (Option<f64>, Option<f64>) {
    world.cache(|cache| (cache.estimated_split_height, cache.estimated_unified_height))
}

fn seed_measured_row_and_position(world: &World) {
    world.instance.with_layout(None, |layout, _| {
        layout.cache.height_deltas.insert(0, 7.0);
        layout.cache.measured_height_delta_total = 7.0;
    });
    assert!(world.position(40).is_some());
    assert!(world.cache(|cache| !cache.checkpoints.is_empty()));
}

fn assert_paired_estimates_cleared(world: &World) {
    assert_eq!(estimates(world), (Some(326.0), Some(346.0)));
    world.cache(|cache| {
        assert_eq!(cache.height_deltas.len(), 0);
        assert_eq!(cache.measured_height_delta_total, 0.0);
        assert!(cache.checkpoints.is_empty());
        assert_eq!(cache.total_lines, 0);
    });
}

#[test]
fn estimated_height_cache_reuses_paired_estimates_across_split_and_unified_style_changes() {
    let world = world(Options::default(), METRICS);
    world.layout(&create_two_hunk_diff("base"), None);
    seed_measured_row_and_position(&world);
    world.set_options(|options| options.base.diff_style = Some(DiffStyle::Unified));

    assert_paired_estimates_cleared(&world);
    assert_eq!(world.height(), 346.0);

    world.set_options(|options| options.base.diff_style = Some(DiffStyle::Split));

    assert_eq!(estimates(&world), (Some(326.0), Some(346.0)));
    assert_eq!(world.height(), 326.0);
}

#[test]
fn estimated_height_cache_keeps_paired_estimates_across_collapse_changes() {
    let world = world(Options::default(), METRICS);
    world.layout(&create_two_hunk_diff("base"), None);
    seed_measured_row_and_position(&world);
    world.set_options(|options| options.base.base.collapsed = Some(true));

    assert_paired_estimates_cleared(&world);
    assert_eq!(world.height(), DIFF_HEADER_HEIGHT);

    world.set_options(|options| options.base.base.collapsed = Some(false));

    assert_eq!(estimates(&world), (Some(326.0), Some(346.0)));
    assert_eq!(world.height(), 326.0);
}

#[test]
fn estimated_height_cache_recomputes_paired_estimates_when_hunk_expansion_changes() {
    let world = world(Options::default(), METRICS);
    world.layout(&create_two_hunk_diff("base"), None);
    seed_measured_row_and_position(&world);
    world.expand_hunk(0, ExpansionDirections::Down, 5);

    assert_eq!(estimates(&world), (Some(376.0), Some(396.0)));
    world.cache(|cache| {
        assert_eq!(cache.height_deltas.len(), 0);
        assert_eq!(cache.measured_height_delta_total, 0.0);
        assert!(cache.checkpoints.is_empty());
        assert_eq!(cache.total_lines, 0);
    });
    assert_eq!(world.height(), 376.0);
}

#[test]
fn estimated_height_cache_applies_measured_height_deltas_without_replaying_full_diff_layout() {
    let mut options = Options::default();
    options.base.base.overflow = Some(Overflow::Wrap);
    let world = world(options, METRICS);
    let file_diff = create_two_hunk_diff("base");
    world.layout(&file_diff, None);
    let measured = measured_group(&world.dom, "0,0", 17.0, None, None);
    world.install(&file_diff, Some(&measured));

    assert!(world.instance.reconcile_heights());
    world.cache(|cache| {
        assert_eq!(cache.height_deltas.get(&0), Some(&7.0));
        assert_eq!(cache.measured_height_delta_total, 7.0);
        assert_eq!(cache.total_lines, 0);
        assert!(cache.checkpoints.is_empty());
    });
    assert_eq!(world.height(), 333.0);

    world.dom.heights.borrow_mut().insert(measured.line, 10.0);

    assert!(world.instance.reconcile_heights());
    world.cache(|cache| {
        assert_eq!(cache.height_deltas.len(), 0);
        assert_eq!(cache.measured_height_delta_total, 0.0);
        assert_eq!(cache.total_lines, 0);
        assert!(cache.checkpoints.is_empty());
    });
    assert_eq!(world.height(), 326.0);
}

#[test]
fn estimated_height_cache_does_not_reserve_unmeasured_file_level_annotations_above_diff_source_rows()
 {
    let world = world(Options::default(), METRICS);
    let line_count = 1_000_000;

    world.layout(
        &create_huge_single_block_diff(line_count),
        Some(&file_annotation()),
    );

    assert_eq!(
        world.height(),
        DIFF_HEADER_HEIGHT + line_count as f64 * LINE_HEIGHT + SPACING
    );
    assert_eq!(world.position(0), None);
    assert_eq!(
        world.position(900_000),
        Some((DIFF_HEADER_HEIGHT + 899_999.0 * LINE_HEIGHT, LINE_HEIGHT))
    );
}

#[test]
fn estimated_height_cache_uses_a_top_render_range_when_measured_file_level_annotations_are_visible()
{
    let world = world(Options::default(), METRICS);
    let file_diff = create_two_hunk_diff("base");
    world.layout(&file_diff, Some(&file_annotation()));
    world.set_cache_for_annotation(25.0);

    let range = world.window(
        &file_diff,
        DIFF_HEADER_HEIGHT + 1.0,
        DIFF_HEADER_HEIGHT + 24.0,
    );

    assert_eq!(range.starting_line, 0.0);
    assert!(range.total_lines > 0.0);
    assert_eq!(range.buffer_before, 0.0);
}

#[test]
fn estimated_height_cache_does_not_use_file_level_annotation_height_to_force_top_render_ranges_when_only_the_header_is_visible()
 {
    let world = world(Options::default(), METRICS);
    let file_diff = create_two_hunk_diff("base");
    world.layout(&file_diff, Some(&file_annotation()));
    world.set_cache_for_annotation(25.0);

    let range = world.window(&file_diff, 1.0, DIFF_HEADER_HEIGHT);

    assert_eq!(range.total_lines, 0.0);
}

#[test]
fn estimated_height_cache_includes_top_render_range_when_the_first_diff_row_is_visible_with_a_zero_height_file_level_annotation()
 {
    let world = world(Options::default(), METRICS);
    let file_diff = create_huge_single_block_diff(1_000_000);
    world.layout(&file_diff, Some(&file_annotation()));

    let range = world.window(
        &file_diff,
        DIFF_HEADER_HEIGHT,
        DIFF_HEADER_HEIGHT + LINE_HEIGHT,
    );

    assert_eq!(range.starting_line, 0.0);
    assert!(range.total_lines > 0.0);
}

#[test]
fn estimated_height_cache_uses_a_final_render_range_when_only_the_synthetic_bottom_separator_is_visible()
 {
    let line_count = 8;
    let file_diff = create_huge_single_block_diff(line_count);
    let world = world(
        Options {
            has_diff_file_loader: true,
            ..Options::default()
        },
        METRICS,
    );

    world.layout(&file_diff, None);
    let separator_top = DIFF_HEADER_HEIGHT + line_count as f64 * LINE_HEIGHT;
    let range = world.window(
        &file_diff,
        separator_top + SPACING + 1.0,
        separator_top + SPACING + 2.0,
    );

    assert_eq!(range.starting_line, 4.0);
    assert!(range.total_lines > 0.0);
}

#[test]
fn estimated_height_cache_does_not_require_measured_file_level_annotation_height_to_render_top_content()
 {
    let world = world(Options::default(), METRICS);
    let file_diff = create_huge_single_block_diff(1_000_000);
    world.layout(&file_diff, Some(&file_annotation()));
    world
        .instance
        .with_layout(None, |layout, _| layout.cache.file_annotation_height = 0.0);

    let range = world.window(
        &file_diff,
        DIFF_HEADER_HEIGHT + 1.0,
        DIFF_HEADER_HEIGHT + 2.0,
    );

    assert_eq!(range.starting_line, 0.0);
    assert!(range.total_lines > 0.0);
    assert_eq!(range.buffer_before, 0.0);
}

#[test]
fn estimated_height_cache_uses_a_top_render_range_for_no_hunk_diffs_with_file_level_annotations() {
    let world = world(Options::default(), METRICS);
    let file_diff = Rc::new(
        serde_json::from_value::<FileDiffMetadata>(serde_json::json!({
            "name": "renamed.ts", "prevName": "old-name.ts", "type": "rename-pure",
            "hunks": [], "splitLineCount": 0, "unifiedLineCount": 0, "isPartial": false,
            "deletionLines": [], "additionLines": [],
            "deletionCharacterCount": 0, "additionCharacterCount": 0,
        }))
        .expect("upstream's no-hunk diff"),
    );
    assert_eq!(file_diff.change_type, ChangeTypes::RenamePure);
    world.layout(&file_diff, Some(&file_annotation()));

    let range = world.window(&file_diff, 0.0, 1.0);

    assert_eq!(range.starting_line, 0.0);
    assert!(range.total_lines > 0.0);
}

pub(super) fn annotated_world() -> (World, Rc<FileDiffMetadata>, Measured) {
    let world = world(Options::default(), METRICS);
    let file_diff = create_two_hunk_diff("base");
    world.layout(&file_diff, Some(&file_annotation()));
    let measured = measured_group(&world.dom, "0,0", LINE_HEIGHT, Some(25.0), None);
    world.install(&file_diff, Some(&measured));
    world.render_range(0.0);
    (world, file_diff, measured)
}

fn assert_annotation(world: &World, height: f64, total: f64) {
    world.cache(|cache| {
        assert_eq!(cache.file_annotation_height, height);
        assert_eq!(cache.measured_height_delta_total, total);
    });
}

#[test]
fn estimated_height_cache_applies_measured_file_level_annotation_height() {
    let (world, _, measured) = annotated_world();

    assert_eq!(world.height(), 326.0);
    assert_annotation(&world, 0.0, 0.0);
    assert!(world.instance.reconcile_heights());
    assert_eq!(world.height(), 351.0);
    assert_annotation(&world, 25.0, 25.0);

    world
        .dom
        .heights
        .borrow_mut()
        .insert(measured.file_annotation.expect("annotation"), LINE_HEIGHT);

    assert!(world.instance.reconcile_heights());
    assert_eq!(world.height(), 336.0);
    assert_annotation(&world, LINE_HEIGHT, LINE_HEIGHT);
}

#[test]
fn estimated_height_cache_ignores_first_row_annotation_keys_when_measuring_file_level_annotation_height()
 {
    let world = world(Options::default(), METRICS);
    let file_diff = create_two_hunk_diff("base");
    world.layout(&file_diff, Some(&file_annotation()));
    let measured = measured_group(&world.dom, "0,0", LINE_HEIGHT, Some(25.0), Some(100.0));
    world.install(&file_diff, Some(&measured));
    world.render_range(0.0);

    assert!(world.instance.reconcile_heights());
    assert_annotation(&world, 25.0, 25.0);
    assert_eq!(world.height(), 351.0);
}

#[test]
fn estimated_height_cache_preserves_measured_file_level_annotation_height_when_the_row_is_not_rendered()
 {
    let (world, file_diff, _) = annotated_world();

    assert!(world.instance.reconcile_heights());
    assert_annotation(&world, 25.0, 25.0);

    world.render_range(HUNK_LINE_COUNT);
    let plain = measured_group(&world.dom, "0,0", LINE_HEIGHT, None, None);
    world.set_additions(&file_diff, &plain);

    assert!(!world.instance.reconcile_heights());
    assert_annotation(&world, 25.0, 25.0);
    assert_eq!(world.height(), 351.0);
}

#[test]
fn estimated_height_cache_clears_measured_file_level_annotation_height_when_the_expected_row_is_missing()
 {
    let (world, file_diff, _) = annotated_world();

    assert!(world.instance.reconcile_heights());
    assert_annotation(&world, 25.0, 25.0);

    let plain = measured_group(&world.dom, "0,0", LINE_HEIGHT, None, None);
    world.set_additions(&file_diff, &plain);

    assert!(world.instance.reconcile_heights());
    assert_annotation(&world, 0.0, 0.0);
    assert_eq!(world.height(), 326.0);
}

#[test]
fn estimated_height_cache_retains_removed_file_level_annotation_height_until_the_top_renders() {
    let (world, file_diff, _) = annotated_world();

    assert!(world.instance.reconcile_heights());
    assert_annotation(&world, 25.0, 25.0);
    assert_eq!(world.height(), 351.0);

    world.layout(
        &file_diff,
        Some(&annotations(&[(AnnotationSide::Additions, 1)])),
    );

    assert_annotation(&world, 25.0, 25.0);
    assert_eq!(world.height(), 351.0);
    world.render_range(HUNK_LINE_COUNT);
    let second = measured_group(&world.dom, "1,1", LINE_HEIGHT, None, None);
    world.set_additions(&file_diff, &second);
    assert!(!world.instance.reconcile_heights());
    assert_annotation(&world, 25.0, 25.0);
    assert_eq!(world.height(), 351.0);

    world.render_range(0.0);
    let first = measured_group(&world.dom, "0,0", LINE_HEIGHT, None, None);
    world.set_additions(&file_diff, &first);
    assert!(world.instance.reconcile_heights());
    assert_annotation(&world, 0.0, 0.0);
    assert_eq!(world.height(), 326.0);
}

#[test]
fn estimated_height_cache_builds_layout_checkpoints_lazily_for_deep_geometry_lookups() {
    let mut options = Options::default();
    options.base.expand_unchanged = Some(true);
    let world = world(options, METRICS);
    let old_lines: Vec<String> = (1..=12_000).map(|index| index.to_string()).collect();
    let mut new_lines = old_lines.clone();
    new_lines[5_999] = "changed-6000".to_owned();
    let file_diff = parse_files(
        plain_file("large.ts", &format!("{}\n", old_lines.join("\n"))),
        plain_file("large.ts", &format!("{}\n", new_lines.join("\n"))),
    );
    world.layout(&file_diff, None);
    let estimated_height = world.height();

    world.cache(|cache| {
        assert_eq!(cache.total_lines, 0);
        assert!(cache.checkpoints.is_empty());
    });

    assert!(world.position(10_000).is_some());

    assert_eq!(world.height(), estimated_height);
    world.cache(|cache| {
        assert!(cache.total_lines > 10_000);
        assert!(cache.checkpoints.len() > 1);
    });
}

#[test]
fn estimated_height_cache_checkpoint_generation_jumps_through_large_uniform_blocks() {
    let world = world(Options::default(), METRICS);
    let line_count = 1_000_000;
    world.layout(&create_huge_single_block_diff(line_count), None);

    assert_eq!(
        world.position(900_000),
        Some((DIFF_HEADER_HEIGHT + 899_999.0 * LINE_HEIGHT, LINE_HEIGHT))
    );
    world.cache(|cache| {
        assert_eq!(cache.total_lines, line_count);
        assert_eq!(
            cache.checkpoints.len(),
            (line_count - 1) / VIRTUALIZED_FILE_DIFF_LAYOUT_CHECKPOINT_INTERVAL + 1
        );
    });
}
