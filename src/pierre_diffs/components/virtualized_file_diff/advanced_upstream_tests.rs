//! Upstream's `VirtualizedFileDiff` cases that need the advanced (CodeView) mode
//! (C1), ported with their names (prefixed by their `describe`):
//! * `test/advancedStickySpecs.test.ts`, `describe('VirtualizedFileDiff.getAdvancedStickySpecs')`
//!   (the four `VirtualizedFile` cases are DEFER with file items). Upstream stubs
//!   the virtualizer as `type: 'simple'` there, and so do these (the method reads
//!   no mode).
//! * `test/VirtualizedFileDiff.partialHydration.test.ts`: thirteen of the
//!   eighteen cases (the loop over `simple`/`advanced` and `disableLineMatching`
//!   is four). The other five need the edit session (`__attachEditor`,
//!   `__prepareForEditing`) or the worker pool, which are OUT. The stub
//!   `createVirtualizer()` that records `instanceChanged` is the real
//!   `Virtualizer` with its test-only `instance_changed_calls`;
//!   `createAdvancedVirtualizer()` is `StubCodeView`, an `AdvancedVirtualizer`.
//!   `loadDiffFiles` is host-driven (`LoaderHarness`), the awaits are gone, and
//!   a diff is an immutable `Rc`: where upstream observes `partial` turn full
//!   after `updateCodeViewLayout`, the test reads the hydrated `Rc` the layout
//!   update returns (`CodeViewLayoutUpdate::hydrated_diff`) and `partial` stays
//!   partial (parity/divergences.psv). Test-only.

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;

use super::advanced::AdvancedVirtualizer;
use super::advanced::VirtualizerKind;
use super::layout_upstream_tests::METRICS;
use super::layout_upstream_tests::parse_files;
use super::layout_upstream_tests::plain_file;
use super::window_upstream_tests::Instance;
use super::window_upstream_tests::Options;
use super::window_upstream_tests::World;
use super::window_upstream_tests::world;
use super::window_upstream_tests::world_with;
use crate::components::file_diff::FileDiffRenderProps;
use crate::components::file_diff_hydration_upstream_tests::LoaderHarness;
use crate::components::file_diff_hydration_upstream_tests::changed;
use crate::components::file_diff_hydration_upstream_tests::create_partial_change;
use crate::components::file_diff_hydration_upstream_tests::keyed_file;
use crate::components::file_diff_hydration_upstream_tests::panic_message;
use crate::components::file_diff_hydration_upstream_tests::parse_single_partial_file;
use crate::components::virtualizer::InstanceRef;
use crate::types::layout::RenderWindow;
use crate::types::layout::StickySpecs;
use crate::types::layout::VirtualWindowSpecs;
use crate::types::lines::ExpansionDirections;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParseDiffOptions;
use crate::utils::compose_cache_key::compose_cache_key;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use crate::utils::compute_virtual_file_metrics::compute_virtual_file_metrics;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_header_region;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_padding_bottom;

/// `createAdvancedVirtualizer()`: the `CodeView` an item talks to, as far as
/// `VirtualizedFileDiff` reads it.
pub(super) struct StubCodeView {
    pub(super) instance_changed_calls: RefCell<Vec<bool>>,
    pub(super) window: Cell<VirtualWindowSpecs>,
    pub(super) local_top: Cell<f64>,
}

impl StubCodeView {
    pub(super) fn new() -> Rc<Self> {
        Rc::new(Self {
            instance_changed_calls: RefCell::default(),
            window: Cell::new(VirtualWindowSpecs {
                top: 0.0,
                bottom: 1_000.0,
            }),
            local_top: Cell::new(0.0),
        })
    }
}

impl AdvancedVirtualizer for StubCodeView {
    fn instance_changed(&self, _instance: &InstanceRef, layout_dirty: bool) {
        self.instance_changed_calls.borrow_mut().push(layout_dirty);
    }

    fn get_window_specs(&self) -> VirtualWindowSpecs {
        self.window.get()
    }

    fn get_local_top_for_instance(&self, _instance: &InstanceRef) -> f64 {
        self.local_top.get()
    }
}

/// An instance under a `StubCodeView` (`createAdvancedVirtualizer()`).
pub(super) fn advanced_world(options: Options) -> (World, Rc<StubCodeView>) {
    let code_view = StubCodeView::new();
    let weak = Rc::downgrade(&code_view);
    let weak: Weak<dyn AdvancedVirtualizer> = weak;
    let world = world_with(options, PartialVirtualFileMetrics::default(), |_| {
        VirtualizerKind::Advanced(weak)
    });
    (world, code_view)
}

// ---- advancedStickySpecs.test.ts ----------------------------------------------

const STICKY_METRICS: PartialVirtualFileMetrics = PartialVirtualFileMetrics {
    hunk_line_count: Some(50),
    line_height: Some(20.0),
    diff_header_height: Some(44.0),
    spacing: Some(8.0),
    hunk_separator_height: None,
    padding_top: None,
    padding_bottom: None,
};
const ITEM_TOP: f64 = 1_000.0;

/// `HEADER_ONLY_HEIGHT`: a header-only item reports just its header region plus
/// the trailing padding, regardless of how much (collapsed) content it has.
fn header_only_height() -> f64 {
    let metrics = compute_virtual_file_metrics(STICKY_METRICS);
    get_virtual_file_header_region(&metrics, false) + get_virtual_file_padding_bottom(&metrics)
}

fn sticky_diff() -> Rc<FileDiffMetadata> {
    parse_files(
        plain_file("example.ts", "one\ntwo\nthree\n"),
        plain_file("example.ts", "one\ntwo changed\nthree\n"),
    )
}

/// A window entirely above the item: the item's content is below the window,
/// so the header renders at the item's top.
fn trailing_window() -> RenderWindow {
    RenderWindow {
        top: 0.0,
        bottom: ITEM_TOP - 1.0,
    }
}

/// A window entirely below the item: the item's content is above the window, so
/// the header must sit at the item's bottom so the next item connects.
fn leading_window(height: f64) -> RenderWindow {
    RenderWindow {
        top: ITEM_TOP + height + 1.0,
        bottom: ITEM_TOP + height + 100.0,
    }
}

fn sticky_instance(options: Options) -> (World, f64) {
    let world = world(options, STICKY_METRICS);
    let height = world
        .instance
        .update_code_view_layout(&sticky_diff(), ITEM_TOP, None, None)
        .height;
    (world, height)
}

#[test]
fn virtualized_file_diff_get_advanced_sticky_specs_reports_a_fully_rendered_item_at_its_top_with_its_full_height()
 {
    let (world, height) = sticky_instance(Options::default());

    assert_eq!(
        world.instance.get_advanced_sticky_specs(Some(RenderWindow {
            top: ITEM_TOP,
            bottom: ITEM_TOP + height,
        })),
        Some(StickySpecs {
            top_offset: ITEM_TOP,
            height,
        })
    );
}

#[test]
fn virtualized_file_diff_get_advanced_sticky_specs_anchors_a_trailing_header_only_item_at_its_top_no_buffer_after()
 {
    let (world, _) = sticky_instance(Options::default());

    assert_eq!(
        world
            .instance
            .get_advanced_sticky_specs(Some(trailing_window())),
        Some(StickySpecs {
            top_offset: ITEM_TOP,
            height: header_only_height(),
        })
    );
}

#[test]
fn virtualized_file_diff_get_advanced_sticky_specs_anchors_a_leading_header_only_item_at_its_bottom_offset_by_buffer_after()
 {
    let (world, height) = sticky_instance(Options::default());
    let buffer_after = height - header_only_height();

    let specs = world
        .instance
        .get_advanced_sticky_specs(Some(leading_window(height)));
    assert_eq!(
        specs,
        Some(StickySpecs {
            top_offset: ITEM_TOP + buffer_after,
            height: header_only_height(),
        })
    );
    // The region ends exactly at the item's logical bottom so the next item
    // connects contiguously.
    let specs = specs.expect("specs");
    assert_eq!(specs.top_offset + specs.height, ITEM_TOP + height);
}

#[test]
fn virtualized_file_diff_get_advanced_sticky_specs_reports_a_collapsed_item_as_its_full_header_height_at_its_top()
 {
    let mut options = Options::default();
    options.base.base.collapsed = Some(true);
    let (world, height) = sticky_instance(options);

    assert_eq!(
        world
            .instance
            .get_advanced_sticky_specs(Some(trailing_window())),
        Some(StickySpecs {
            top_offset: ITEM_TOP,
            height,
        })
    );
}

// ---- VirtualizedFileDiff.partialHydration.test.ts ------------------------------

fn partial_options(configure: impl FnOnce(&mut Options)) -> Options {
    let mut options = Options::default();
    options.base.base.disable_file_header = Some(true);
    options.has_diff_file_loader = true;
    configure(&mut options);
    options
}

/// `createVirtualizer()`: the `instanceChanged` calls its stub records.
fn simple_instance_changed_calls(instance: &Instance) -> Vec<bool> {
    instance
        .virtualizer()
        .simple()
        .expect("a simple virtualizer")
        .instance_changed_calls()
}

fn expanded(instance: &Instance, index: usize) -> (usize, usize) {
    let region = instance
        .file_diff()
        .hunks_renderer()
        .get_expanded_hunk(index);
    (region.from_start, region.from_end)
}

fn input_diff(instance: &Instance) -> Rc<FileDiffMetadata> {
    instance
        .file_diff()
        .get_input_diff()
        .expect("an input diff")
}

fn layout(instance: &Instance, file_diff: &Rc<FileDiffMetadata>) -> f64 {
    instance
        .update_code_view_layout(file_diff, 0.0, None, None)
        .height
}

#[test]
fn virtualized_file_diff_partial_hydration_expand_hunk_hydrates_once_preserves_expansion_state_and_marks_layout_dirty()
 {
    let (old_file, new_file, partial) = create_partial_change("partial.txt");
    let world = world(partial_options(|_| {}), METRICS);
    let instance = &world.instance;
    let loader = LoaderHarness::new(instance.file_diff());

    layout(instance, &partial);
    assert!(Rc::ptr_eq(&input_diff(instance), &partial));
    assert!(instance.file_diff().get_rendered_diff().is_none());
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    instance.expand_hunk(0, ExpansionDirections::Up, Some(1.0));
    loader.poll();

    assert_eq!(loader.calls.get(), 1);
    assert!(Rc::ptr_eq(&loader.in_flight(), &partial));
    assert!(Rc::ptr_eq(&input_diff(instance), &partial));
    assert!(input_diff(instance).is_partial);
    assert_eq!(expanded(instance, 0), (1, 1));
    assert_eq!(simple_instance_changed_calls(instance), [true, true]);

    loader.resolve(&changed(&old_file, &new_file));

    // Upstream hydrates `partial` in place; the instance swaps its `Rc`.
    let hydrated = input_diff(instance);
    assert!(partial.is_partial);
    assert!(!hydrated.is_partial);
    assert_eq!(
        hydrated.addition_lines,
        ["keep 1\n", "new value\n", "keep 3\n", "keep 4\n"]
    );
    assert_eq!(
        hydrated.deletion_lines,
        ["keep 1\n", "old value\n", "keep 3\n", "keep 4\n"]
    );
    assert_eq!(expanded(instance, 0), (1, 1));
    assert_eq!(simple_instance_changed_calls(instance), [true, true, true]);
    instance.clean_up(false);
}

#[test]
fn virtualized_file_diff_partial_hydration_expanding_the_synthetic_bottom_hunk_starts_hydration_and_marks_layout_dirty()
 {
    let (old_file, new_file, partial) = create_partial_change("partial.txt");
    let world = world(partial_options(|_| {}), METRICS);
    let instance = &world.instance;
    let loader = LoaderHarness::new(instance.file_diff());
    let synthetic_hunk = partial.hunks.len() as i64;

    layout(instance, &partial);
    instance.expand_hunk(synthetic_hunk, ExpansionDirections::Up, Some(1.0));
    loader.poll();
    instance.expand_hunk(synthetic_hunk, ExpansionDirections::Up, Some(1.0));
    loader.poll();

    assert_eq!(loader.calls.get(), 1);
    assert!(Rc::ptr_eq(&loader.in_flight(), &partial));
    assert!(Rc::ptr_eq(&input_diff(instance), &partial));
    assert!(input_diff(instance).is_partial);
    assert_eq!(expanded(instance, partial.hunks.len()), (2, 0));
    assert_eq!(simple_instance_changed_calls(instance), [true, true]);

    loader.resolve(&changed(&old_file, &new_file));

    assert!(!input_diff(instance).is_partial);
    assert_eq!(simple_instance_changed_calls(instance), [true, true, true]);
    instance.clean_up(false);
}

#[test]
fn virtualized_file_diff_partial_hydration_ignores_stale_loader_results_after_the_prepared_diff_changes()
 {
    let (old_file, new_file, partial) = create_partial_change("first.txt");
    let next_diff = parse_files(
        plain_file("second.txt", "before\n"),
        plain_file("second.txt", "after\n"),
    );
    let world = world(partial_options(|_| {}), METRICS);
    let instance = &world.instance;
    let loader = LoaderHarness::new(instance.file_diff());

    layout(instance, &partial);
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    layout(instance, &next_diff);

    loader.resolve(&changed(&old_file, &new_file));

    assert!(Rc::ptr_eq(&input_diff(instance), &next_diff));
    assert_eq!(input_diff(instance).name, "second.txt");
    assert!(!input_diff(instance).is_partial);
    assert_eq!(input_diff(instance).addition_lines, ["after\n"]);
    assert_eq!(simple_instance_changed_calls(instance), [true]);
    instance.clean_up(false);
}

#[test]
fn virtualized_file_diff_partial_hydration_rendering_a_partial_diff_after_a_full_diff_replaces_the_external_target()
 {
    let (old_file, new_file, partial) = create_partial_change("partial.txt");
    let full_diff = parse_files(old_file, new_file);
    let world = world(partial_options(|_| {}), METRICS);
    let instance = &world.instance;
    let container = world.dom.doc.create_element("div");

    for file_diff in [&full_diff, &partial] {
        instance.render(FileDiffRenderProps {
            file_container: Some(container),
            file_diff: Some(Rc::clone(file_diff)),
            defer_managers: true,
            prevent_emit: true,
            ..FileDiffRenderProps::default()
        });
    }

    assert!(Rc::ptr_eq(&input_diff(instance), &partial));
    assert!(input_diff(instance).is_partial);
    instance.clean_up(false);
}

#[test]
fn virtualized_file_diff_partial_hydration_prepare_code_view_item_accepts_a_partial_diff_after_a_full_diff()
 {
    let (old_file, new_file, partial) = create_partial_change("partial.txt");
    let full_diff = parse_files(old_file, new_file);
    let world = world(partial_options(|_| {}), METRICS);
    let instance = &world.instance;

    layout(instance, &full_diff);

    let height = layout(instance, &partial);

    assert!(height.is_finite());
    assert!(Rc::ptr_eq(&input_diff(instance), &partial));
    assert!(input_diff(instance).is_partial);
    instance.clean_up(false);
}

#[test]
fn virtualized_file_diff_partial_hydration_applies_pending_code_view_expansions_when_the_prepared_diff_changes()
 {
    let first_change = create_partial_change("first.txt");
    let second_change = create_partial_change("second.txt");
    let (world, code_view) = advanced_world(partial_options(|_| {}));
    let instance = &world.instance;
    let loader = LoaderHarness::new(instance.file_diff());

    layout(instance, &first_change.2);
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    layout(instance, &second_change.2);

    assert_eq!(expanded(instance, 0), (0, 1));
    assert_eq!(*code_view.instance_changed_calls.borrow(), [true]);
    instance.clean_up(false);
}

#[test]
fn virtualized_file_diff_partial_hydration_missing_loader_rejects_a_code_view_expansion_before_staging_it()
 {
    let first_change = create_partial_change("first.txt");
    let second_change = create_partial_change("second.txt");
    let (world, code_view) = advanced_world(partial_options(|options| {
        options.has_diff_file_loader = false;
    }));
    let instance = &world.instance;

    layout(instance, &first_change.2);
    let expanded_before = expanded(instance, 0);
    let message = panic_message(|| instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0)));
    assert!(message.contains("loadDiffFiles is required to load full files"));
    assert!(code_view.instance_changed_calls.borrow().is_empty());

    layout(instance, &second_change.2);
    assert_eq!(expanded(instance, 0), expanded_before);
    instance.clean_up(false);
}

fn derives_hydrated_cache_keys(advanced: bool, disable_line_matching: bool) {
    let (old_file, new_file, partial) = create_partial_change("partial.txt");
    let partial = Rc::new(FileDiffMetadata {
        cache_key: None,
        ..(*partial).clone()
    });
    let options = partial_options(|options| {
        options.base.parse_diff_options = Some(ParseDiffOptions {
            disable_line_matching: Some(disable_line_matching),
            ..ParseDiffOptions::default()
        });
    });
    let (world, _code_view) = if advanced {
        let (world, code_view) = advanced_world(options);
        (world, Some(code_view))
    } else {
        (world(options, METRICS), None)
    };
    let instance = &world.instance;
    let loader = LoaderHarness::new(instance.file_diff());

    layout(instance, &partial);
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    loader.resolve(&changed(&old_file, &new_file));
    let hydrated = if advanced {
        assert!(partial.is_partial);
        assert!(input_diff(instance).is_partial);
        layout(instance, &partial);
        input_diff(instance)
    } else {
        input_diff(instance)
    };

    assert!(!hydrated.is_partial);
    assert_eq!(
        hydrated.cache_key.as_deref(),
        Some(
            compose_cache_key(
                if disable_line_matching {
                    "hydrated-files-raw"
                } else {
                    "hydrated-files"
                },
                &[
                    old_file.cache_key.as_deref().expect("a key"),
                    new_file.cache_key.as_deref().expect("a key"),
                ],
            )
            .as_str()
        )
    );
    instance.clean_up(false);
}

#[test]
fn virtualized_file_diff_partial_hydration_simple_hydration_derives_cache_keys_using_disable_line_matching_false()
 {
    derives_hydrated_cache_keys(false, false);
}

#[test]
fn virtualized_file_diff_partial_hydration_simple_hydration_derives_cache_keys_using_disable_line_matching_true()
 {
    derives_hydrated_cache_keys(false, true);
}

#[test]
fn virtualized_file_diff_partial_hydration_advanced_hydration_derives_cache_keys_using_disable_line_matching_false()
 {
    derives_hydrated_cache_keys(true, false);
}

#[test]
fn virtualized_file_diff_partial_hydration_advanced_hydration_derives_cache_keys_using_disable_line_matching_true()
 {
    derives_hydrated_cache_keys(true, true);
}

#[test]
fn virtualized_file_diff_partial_hydration_commits_staged_code_view_hydration_at_the_layout_consumption_boundary()
 {
    let (old_file, new_file, partial) = create_partial_change("partial.ts");
    let (world, code_view) = advanced_world(partial_options(|_| {}));
    let instance = &world.instance;
    let loader = LoaderHarness::new(instance.file_diff());

    layout(instance, &partial);
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    loader.resolve(&changed(&old_file, &new_file));

    assert!(partial.is_partial);
    assert!(Rc::ptr_eq(&input_diff(instance), &partial));
    assert_eq!(input_diff(instance).addition_lines, ["new value\n"]);
    assert_eq!(*code_view.instance_changed_calls.borrow(), [true, true]);

    let update = instance.update_code_view_layout(&partial, 0.0, None, None);

    // Upstream's `Object.assign(partial, nextDiff)`: the layout update hands the
    // hydrated `Rc` back and the instance holds it as its input.
    let hydrated = update
        .hydrated_diff
        .expect("the staged hydration was consumed");
    assert!(Rc::ptr_eq(&input_diff(instance), &hydrated));
    assert!(partial.is_partial);
    assert!(!hydrated.is_partial);
    assert_eq!(
        hydrated.addition_lines,
        ["keep 1\n", "new value\n", "keep 3\n", "keep 4\n"]
    );
    instance.clean_up(false);
}

#[test]
fn virtualized_file_diff_partial_hydration_expand_unchanged_starts_hydration_for_pure_rename_partial_diffs()
 {
    let partial = parse_single_partial_file(
        "diff --git a/old-name.txt b/new-name.txt\nsimilarity index 100%\nrename from old-name.txt\nrename to new-name.txt\n",
    );
    assert_eq!(partial.change_type, ChangeTypes::RenamePure);
    assert!(partial.hunks.is_empty());
    let new_file = keyed_file("new-name.txt", "alpha\nbeta\n", "rename:new");
    let world = world(
        partial_options(|options| {
            options.base.expand_unchanged = Some(true);
        }),
        METRICS,
    );
    let instance = &world.instance;
    let loader = LoaderHarness::new(instance.file_diff());
    let container = world.dom.doc.create_element("div");

    instance.render(FileDiffRenderProps {
        file_container: Some(container),
        file_diff: Some(Rc::clone(&partial)),
        defer_managers: true,
        prevent_emit: true,
        ..FileDiffRenderProps::default()
    });
    loader.poll();
    assert_eq!(loader.calls.get(), 1);
    assert!(Rc::ptr_eq(&loader.in_flight(), &partial));
    loader.resolve(&FileDiffLoadedFiles::PureRenamed { new_file });

    let hydrated = input_diff(instance);
    assert_eq!(hydrated.change_type, ChangeTypes::RenamePure);
    assert!(!hydrated.is_partial);
    assert_eq!(hydrated.addition_lines, ["alpha\n", "beta\n"]);
    assert_eq!(hydrated.deletion_lines, ["alpha\n", "beta\n"]);
    assert_eq!(simple_instance_changed_calls(instance).last(), Some(&true));
    instance.clean_up(false);
}
