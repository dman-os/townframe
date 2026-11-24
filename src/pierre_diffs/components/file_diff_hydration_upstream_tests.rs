//! Upstream's `FileDiff.partialHydration.test.ts` and the `FileDiff` cases of
//! `themeTypeUpdates.test.ts`, ported with their names over `FakeDom` and the
//! plain-text stub highlighter (the helpers are those of
//! `file_diff_upstream_tests`). `loadDiffFiles` is host-driven, so
//! `LoaderHarness` is the host; the awaits are gone (synchronous); the tests
//! that need the edit session, the worker pool or CodeView are not ported (see
//! parity/divergences.psv). Test-only.

use std::rc::Rc;

use crate::components::file_diff::FileDiffRenderProps;
use crate::diff::PatchOptions;
use crate::diff::PatchSide;
use crate::diff::create_two_files_patch;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::types::layout::RenderRange;
use crate::types::lines::ExpansionDirections;
use crate::types::options::DiffStyle;
use crate::types::options::ThemeOption;
use crate::types::options::ThemeTypes;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParseDiffOptions;
use crate::types::patch::ParsePatchOptions;
use crate::utils::compose_cache_key::compose_cache_key;
use crate::utils::parse_patch_files::parse_patch_files;

use super::file_diff_upstream_tests::Instance;
use super::file_diff_upstream_tests::Options;
use super::file_diff_upstream_tests::file;
use super::file_diff_upstream_tests::instance;
use super::file_diff_upstream_tests::parse;
use super::file_diff_upstream_tests::shadow_root;
use super::file_diff_upstream_tests::window;

// ---- FileDiff.partialHydration.test.ts ---------------------------------------

/// `catch_unwind` of `action`, its panic message (`expect(...).toThrow(message)`).
fn panic_message(action: impl FnOnce()) -> String {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(action))
        .expect_err("expected a panic");
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .expect("panic with a message")
}

fn parse_single_partial_file(patch: &str) -> Rc<FileDiffMetadata> {
    let file = parse_patch_files(
        patch,
        &ParsePatchOptions {
            cache_key_prefix: Some("partial".to_owned()),
            throw_on_error: Some(true),
            ..ParsePatchOptions::default()
        },
    )
    .expect("valid")
    .value
    .remove(0)
    .files
    .remove(0);
    assert!(file.is_partial);
    Rc::new(file)
}

fn keyed_file(name: &str, contents: &str, cache_key: &str) -> FileContents {
    FileContents {
        cache_key: Some(cache_key.to_owned()),
        ..file(name, contents)
    }
}

/// `createPartialChange`: a change in a four-line file with no context, so the
/// first hunk has collapsed lines before it.
fn create_partial_change(name: &str) -> (FileContents, FileContents, Rc<FileDiffMetadata>) {
    let old_file = keyed_file(
        name,
        "keep 1\nold value\nkeep 3\nkeep 4\n",
        &format!("{name}:old"),
    );
    let new_file = keyed_file(
        name,
        "keep 1\nnew value\nkeep 3\nkeep 4\n",
        &format!("{name}:new"),
    );
    let patch = create_two_files_patch(
        PatchSide {
            file_name: &old_file.name,
            contents: &old_file.contents,
            header: None,
        },
        PatchSide {
            file_name: &new_file.name,
            contents: &new_file.contents,
            header: None,
        },
        PatchOptions {
            context: Some(0),
            ..PatchOptions::default()
        },
    );
    let partial = parse_single_partial_file(&patch);
    assert!(partial.hunks[0].collapsed_before > 0);
    (old_file, new_file, partial)
}

fn changed(old_file: &FileContents, new_file: &FileContents) -> FileDiffLoadedFiles {
    FileDiffLoadedFiles::Changed {
        old_file: old_file.clone(),
        new_file: new_file.clone(),
    }
}

/// The host side of `loadDiffFiles`: starts one load per pending request (what
/// `loadFilesForDiff` does when `loadFilesIfNecessary` finds a partial diff),
/// counts the calls, and settles it when the test says so (the `deferred`
/// promise).
struct LoaderHarness {
    instance: Rc<Instance>,
    calls: std::cell::Cell<usize>,
    in_flight: std::cell::RefCell<Option<Rc<FileDiffMetadata>>>,
}

impl LoaderHarness {
    fn new(instance: &Rc<Instance>) -> Self {
        Self {
            instance: Rc::clone(instance),
            calls: std::cell::Cell::new(0),
            in_flight: std::cell::RefCell::new(None),
        }
    }

    /// What the host does after every call that may have asked for files: a
    /// pending request the harness has not started a load for gets one
    /// (`loadDiffFiles(fileDiff)`).
    fn poll(&self) {
        let Some(pending) = self.instance.loading().pending_files() else {
            return;
        };
        let mut in_flight = self.in_flight.borrow_mut();
        if in_flight
            .as_ref()
            .is_some_and(|started| Rc::ptr_eq(started, &pending))
        {
            return;
        }
        self.calls.set(self.calls.get() + 1);
        *in_flight = Some(pending);
    }

    fn in_flight(&self) -> Rc<FileDiffMetadata> {
        Rc::clone(self.in_flight.borrow().as_ref().expect("a load in flight"))
    }

    /// `deferred.resolve(files)`
    fn resolve(&self, files: &FileDiffLoadedFiles) {
        let diff = self
            .in_flight
            .borrow_mut()
            .take()
            .expect("a load in flight");
        self.instance.loading().files_loaded(&diff, files);
    }

    /// `Promise.reject(error)`: the host drops the request.
    fn reject(&self) {
        let diff = self
            .in_flight
            .borrow_mut()
            .take()
            .expect("a load in flight");
        self.instance.loading().abandon_file_load(&diff);
    }
}

fn partial_instance(
    dom: &Rc<FakeDom>,
    configure: impl FnOnce(&mut Options),
) -> (Rc<Instance>, NodeKey) {
    let mut options = Options::default();
    options.base.base.disable_file_header = Some(true);
    configure(&mut options);
    (instance(dom, options), dom.doc.create_element("div"))
}

fn render_deferred(instance: &Instance, container: NodeKey, file_diff: &Rc<FileDiffMetadata>) {
    instance.lifecycle().render(FileDiffRenderProps {
        file_container: Some(container),
        file_diff: Some(Rc::clone(file_diff)),
        defer_managers: true,
        prevent_emit: true,
        ..FileDiffRenderProps::default()
    });
}

fn expanded(instance: &Instance, index: usize) -> (usize, usize) {
    let region = instance.hunks_renderer().get_expanded_hunk(index);
    (region.from_start, region.from_end)
}

fn input_diff(instance: &Instance) -> Rc<FileDiffMetadata> {
    instance.get_input_diff().expect("an input diff")
}

#[test]
fn partial_hydration_expand_hunk_hydrates_once_and_preserves_expansion_state_changes_made_while_loading()
 {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, partial) = create_partial_change("partial.ts");
    let (instance, container) = partial_instance(&dom, |options| {
        options.has_diff_file_loader = true;
    });
    let loader = LoaderHarness::new(&instance);

    render_deferred(&instance, container, &partial);
    loader.poll();
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    instance.expand_hunk(0, ExpansionDirections::Up, Some(2.0));
    loader.poll();

    assert_eq!(loader.calls.get(), 1);
    assert!(Rc::ptr_eq(&loader.in_flight(), &partial));
    assert!(Rc::ptr_eq(&input_diff(&instance), &partial));
    assert!(input_diff(&instance).is_partial);
    assert_eq!(expanded(&instance, 0), (2, 1));

    loader.resolve(&changed(&old_file, &new_file));

    // Changed: upstream hydrates `partial` in place (`toBe(partial)`); the
    // instance swaps its `Rc` for the hydrated copy and `partial` stays partial.
    assert!(partial.is_partial);
    let hydrated = input_diff(&instance);
    assert!(!hydrated.is_partial);
    assert_eq!(
        hydrated.addition_lines,
        ["keep 1\n", "new value\n", "keep 3\n", "keep 4\n"]
    );
    assert_eq!(
        hydrated.deletion_lines,
        ["keep 1\n", "old value\n", "keep 3\n", "keep 4\n"]
    );
    assert_eq!(expanded(&instance, 0), (2, 1));
    instance.lifecycle().clean_up(false);
}

#[test]
fn partial_hydration_expand_hunk_without_a_file_loader_throws_before_changing_the_hunk() {
    let dom = Rc::new(FakeDom::default());
    let (_, _, partial) = create_partial_change("partial.txt");
    let (instance, container) = partial_instance(&dom, |_| {});
    let loader = LoaderHarness::new(&instance);

    render_deferred(&instance, container, &partial);
    let expanded_before = expanded(&instance, 0);
    let message = panic_message(|| instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0)));
    assert!(message.contains("loadDiffFiles is required to load full files"));

    assert!(Rc::ptr_eq(&input_diff(&instance), &partial));
    assert!(input_diff(&instance).is_partial);
    loader.poll();
    assert!(instance.loading().pending_files().is_none());
    assert_eq!(expanded(&instance, 0), expanded_before);
    // `__prepareForEditing` (edit mode) is not ported.
    instance.lifecycle().clean_up(false);
}

fn expect_one_sided_partial_does_not_start_hydration(partial: &Rc<FileDiffMetadata>) {
    let dom = Rc::new(FakeDom::default());
    let (instance, container) = partial_instance(&dom, |options| {
        options.base.expand_unchanged = Some(true);
        options.has_diff_file_loader = true;
    });
    let loader = LoaderHarness::new(&instance);

    render_deferred(&instance, container, partial);
    loader.poll();
    assert_eq!(loader.calls.get(), 0);
    assert!(instance.loading().pending_files().is_none());

    instance.expand_hunk(0, ExpansionDirections::Both, Some(1.0));
    loader.poll();
    assert_eq!(loader.calls.get(), 0);
    assert!(Rc::ptr_eq(&input_diff(&instance), partial));
    assert!(input_diff(&instance).is_partial);
    assert!(instance.loading().pending_files().is_none());
    // `__prepareForEditing` / `prepareForEditing` (edit mode) are not ported.
    instance.lifecycle().clean_up(false);
}

#[test]
fn partial_hydration_expand_unchanged_and_expand_hunk_do_not_hydrate_added_or_deleted_partial_diffs()
 {
    expect_one_sided_partial_does_not_start_hydration(&parse_single_partial_file(
        "diff --git a/new-file.txt b/new-file.txt\nnew file mode 100644\nindex 0000000..1111111\n--- /dev/null\n+++ b/new-file.txt\n@@ -0,0 +1,2 @@\n+alpha\n+beta\n",
    ));
    expect_one_sided_partial_does_not_start_hydration(&parse_single_partial_file(
        "diff --git a/deleted-file.txt b/deleted-file.txt\ndeleted file mode 100644\nindex 1111111..0000000\n--- a/deleted-file.txt\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-alpha\n-beta\n",
    ));
}

#[test]
fn partial_hydration_ignores_loaded_files_if_the_source_diff_is_already_full() {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, partial) = create_partial_change("partial.ts");
    let (instance, _) = partial_instance(&dom, |_| {});

    // `instance.fileDiff = partial; partial.isPartial = false;` (the `Rc` is
    // immutable, so the already-full diff is a copy). The worker manager's
    // `cleanUpTasks` assertion is not ported.
    let already_full = Rc::new(FileDiffMetadata {
        is_partial: false,
        ..(*partial).clone()
    });
    instance.state().file_diff = Some(Rc::clone(&already_full));

    instance
        .loading()
        .files_loaded(&already_full, &changed(&old_file, &new_file));

    assert!(Rc::ptr_eq(&input_diff(&instance), &already_full));
    assert!(!already_full.is_partial);
    assert!(instance.state().deletion_file.is_none());
    assert!(instance.state().addition_file.is_none());
    instance.lifecycle().clean_up(false);
}

fn synthetic_bottom_separator(
    dom: &FakeDom,
    container: NodeKey,
    hunk_index: usize,
) -> Option<NodeKey> {
    dom.doc
        .query_by_attribute(
            shadow_root(dom, container),
            "data-expand-index",
            Some(&hunk_index.to_string()),
        )
        .into_iter()
        .find_map(|separator| {
            dom.doc
                .query_by_attribute(separator, "data-expand-up", None)
                .first()
                .copied()
        })
}

#[test]
fn partial_hydration_expanding_the_synthetic_bottom_hunk_hydrates_once_and_clears_partial_only_ui()
{
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, partial) = create_partial_change("partial.ts");
    let (instance, container) = partial_instance(&dom, |options| {
        options.base.diff_style = Some(DiffStyle::Unified);
        options.hunk_separators =
            Some(crate::components::file_diff::FileDiffHunkSeparators::LineInfo);
        options.has_diff_file_loader = true;
    });
    let loader = LoaderHarness::new(&instance);
    let text = || dom.doc.text_content(shadow_root(&dom, container));

    instance.lifecycle().render(FileDiffRenderProps {
        file_container: Some(container),
        file_diff: Some(Rc::clone(&partial)),
        prevent_emit: true,
        ..FileDiffRenderProps::default()
    });
    loader.poll();
    assert!(synthetic_bottom_separator(&dom, container, partial.hunks.len()).is_some());
    assert!(text().contains("More unchanged context may be available"));

    instance.expand_hunk(partial.hunks.len() as i64, ExpansionDirections::Up, None);
    loader.poll();
    assert_eq!(loader.calls.get(), 1);
    assert!(instance.loading().pending_files().is_some());

    instance.expand_hunk(partial.hunks.len() as i64, ExpansionDirections::Up, None);
    loader.poll();
    assert_eq!(loader.calls.get(), 1);

    loader.resolve(&changed(&old_file, &new_file));

    assert!(!input_diff(&instance).is_partial);
    assert!(!text().contains("More unchanged context may be available"));
    assert!(synthetic_bottom_separator(&dom, container, partial.hunks.len()).is_none());
    assert!(instance.loading().pending_files().is_none());
    instance.lifecycle().clean_up(false);
}

#[test]
fn partial_hydration_expand_hunk_on_full_diffs_does_not_start_partial_hydration() {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, _) = create_partial_change("partial.txt");
    let full_diff = parse(old_file, new_file);
    let (instance, container) = partial_instance(&dom, |options| {
        options.has_diff_file_loader = true;
    });
    let loader = LoaderHarness::new(&instance);

    render_deferred(&instance, container, &full_diff);
    instance.expand_hunk(0, ExpansionDirections::Both, Some(1.0));
    loader.poll();

    assert_eq!(loader.calls.get(), 0);
    assert!(Rc::ptr_eq(&input_diff(&instance), &full_diff));
    assert!(!input_diff(&instance).is_partial);
    assert!(instance.loading().pending_files().is_none());
    assert_eq!(expanded(&instance, 0), (1, 1));
    instance.lifecycle().clean_up(false);
}

#[test]
fn partial_hydration_rendering_a_partial_diff_after_a_full_diff_replaces_the_rendered_metadata() {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, partial) = create_partial_change("partial.txt");
    let full_diff = parse(old_file, new_file);
    let (instance, container) = partial_instance(&dom, |_| {});

    render_deferred(&instance, container, &full_diff);
    let did_render = instance.lifecycle().render(FileDiffRenderProps {
        file_container: Some(container),
        file_diff: Some(Rc::clone(&partial)),
        defer_managers: true,
        prevent_emit: true,
        ..FileDiffRenderProps::default()
    });

    assert!(did_render);
    assert!(Rc::ptr_eq(&input_diff(&instance), &partial));
    assert!(input_diff(&instance).is_partial);
    instance.lifecycle().clean_up(false);
}

const HYDRATE_ONCE: &str =
    "FileDiff.hydrate: hydrate can only be called before the instance has rendered or hydrated";

#[test]
fn partial_hydration_hydrate_can_only_initialize_a_file_diff_instance() {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, _) = create_partial_change("partial.txt");
    let full_diff = parse(old_file, new_file);
    let (instance, container) = partial_instance(&dom, |_| {});
    let hydrate = || {
        instance.lifecycle().hydrate(FileDiffRenderProps {
            file_container: Some(container),
            file_diff: Some(Rc::clone(&full_diff)),
            prevent_emit: true,
            ..FileDiffRenderProps::default()
        });
    };

    hydrate();
    assert!(panic_message(hydrate).contains(HYDRATE_ONCE));
    instance.lifecycle().clean_up(false);
}

#[test]
fn partial_hydration_hydrate_after_render_rejects_file_diff_updates() {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, _) = create_partial_change("partial.txt");
    let full_diff = parse(old_file, new_file);
    let (instance, container) = partial_instance(&dom, |_| {});

    render_deferred(&instance, container, &full_diff);
    let message = panic_message(|| {
        instance.lifecycle().hydrate(FileDiffRenderProps {
            file_container: Some(container),
            file_diff: Some(Rc::clone(&full_diff)),
            prevent_emit: true,
            ..FileDiffRenderProps::default()
        });
    });

    assert!(message.contains(HYDRATE_ONCE));
    instance.lifecycle().clean_up(false);
}

#[test]
fn partial_hydration_ignores_stale_loader_results_after_the_rendered_diff_changes() {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, partial) = create_partial_change("first.txt");
    let next_diff = parse(
        file("second.txt", "before\n"),
        file("second.txt", "after\n"),
    );
    let (instance, container) = partial_instance(&dom, |options| {
        options.has_diff_file_loader = true;
    });
    let loader = LoaderHarness::new(&instance);

    render_deferred(&instance, container, &partial);
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    render_deferred(&instance, container, &next_diff);

    loader.resolve(&changed(&old_file, &new_file));

    assert!(Rc::ptr_eq(&input_diff(&instance), &next_diff));
    assert_eq!(input_diff(&instance).name, "second.txt");
    assert!(!input_diff(&instance).is_partial);
    assert_eq!(input_diff(&instance).addition_lines, ["after\n"]);
    instance.lifecycle().clean_up(false);
}

fn derives_hydrated_cache_keys(disable_line_matching: bool) {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, partial) = create_partial_change("partial.txt");
    let partial = Rc::new(FileDiffMetadata {
        cache_key: None,
        ..(*partial).clone()
    });
    let (instance, container) = partial_instance(&dom, |options| {
        options.base.parse_diff_options = Some(ParseDiffOptions {
            disable_line_matching: Some(disable_line_matching),
            ..ParseDiffOptions::default()
        });
        options.has_diff_file_loader = true;
    });
    let loader = LoaderHarness::new(&instance);

    render_deferred(&instance, container, &partial);
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    loader.resolve(&changed(&old_file, &new_file));

    assert!(partial.is_partial);
    assert!(!input_diff(&instance).is_partial);
    assert_eq!(
        input_diff(&instance).cache_key.as_deref(),
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
    instance.lifecycle().clean_up(false);
}

#[test]
fn partial_hydration_derives_hydrated_cache_keys_using_disable_line_matching_false() {
    derives_hydrated_cache_keys(false);
}

#[test]
fn partial_hydration_derives_hydrated_cache_keys_using_disable_line_matching_true() {
    derives_hydrated_cache_keys(true);
}

#[test]
fn partial_hydration_expand_unchanged_starts_hydration_for_pure_rename_partial_diffs() {
    let dom = Rc::new(FakeDom::default());
    let partial = parse_single_partial_file(
        "diff --git a/old-name.txt b/new-name.txt\nsimilarity index 100%\nrename from old-name.txt\nrename to new-name.txt\n",
    );
    assert_eq!(partial.change_type, ChangeTypes::RenamePure);
    assert!(partial.hunks.is_empty());
    let new_file = keyed_file("new-name.txt", "alpha\nbeta\n", "rename:new");
    let (instance, container) = partial_instance(&dom, |options| {
        options.base.expand_unchanged = Some(true);
        options.has_diff_file_loader = true;
    });
    let loader = LoaderHarness::new(&instance);

    render_deferred(&instance, container, &partial);
    loader.poll();
    assert_eq!(loader.calls.get(), 1);
    assert!(Rc::ptr_eq(&loader.in_flight(), &partial));
    loader.resolve(&FileDiffLoadedFiles::PureRenamed { new_file });

    let hydrated = input_diff(&instance);
    assert_eq!(hydrated.change_type, ChangeTypes::RenamePure);
    assert!(!hydrated.is_partial);
    assert_eq!(hydrated.addition_lines, ["alpha\n", "beta\n"]);
    assert_eq!(hydrated.deletion_lines, ["alpha\n", "beta\n"]);
    instance.lifecycle().clean_up(false);
}

#[test]
fn partial_hydration_logs_loader_errors_keeps_partial_diff_intact_and_allows_retry() {
    let dom = Rc::new(FakeDom::default());
    let (old_file, new_file, partial) = create_partial_change("partial.txt");
    let (instance, container) = partial_instance(&dom, |options| {
        options.has_diff_file_loader = true;
    });
    let loader = LoaderHarness::new(&instance);

    render_deferred(&instance, container, &partial);
    instance.expand_hunk(0, ExpansionDirections::Down, Some(1.0));
    loader.poll();
    // The first load fails: the host owns the error (upstream's `console.error`
    // is not ported) and drops the request.
    loader.reject();

    assert_eq!(loader.calls.get(), 1);
    assert!(Rc::ptr_eq(&input_diff(&instance), &partial));
    assert!(input_diff(&instance).is_partial);
    assert!(instance.loading().pending_files().is_none());

    instance.expand_hunk(0, ExpansionDirections::Up, Some(1.0));
    loader.poll();
    assert_eq!(loader.calls.get(), 2);
    loader.resolve(&changed(&old_file, &new_file));

    assert!(!input_diff(&instance).is_partial);
    assert_eq!(
        input_diff(&instance).addition_lines,
        ["keep 1\n", "new value\n", "keep 3\n", "keep 4\n"]
    );
    instance.lifecycle().clean_up(false);
}

// ---- themeTypeUpdates.test.ts (the FileDiff cases) -------------------------

fn make_long_file(name: &str, changed_value: usize) -> FileContents {
    let lines: String = (0..20)
        .map(|index| {
            let value = if index == 8 { changed_value } else { index };
            format!("const value{index} = {value};\n")
        })
        .collect();
    keyed_file(name, &lines, &format!("{name}:{changed_value}"))
}

fn theme_style(dom: &FakeDom, container: NodeKey) -> NodeKey {
    *dom.doc
        .query_by_attribute(shadow_root(dom, container), "data-theme-css", None)
        .first()
        .expect("a theme style element")
}

fn assert_color_scheme(dom: &FakeDom, style: NodeKey, scheme: &str, other: &str) {
    let text = dom.doc.text_content(style);
    assert!(text.contains(&format!("color-scheme: {scheme};")), "{text}");
    assert!(!text.contains(&format!("color-scheme: {other};")), "{text}");
}

fn paired_theme_options() -> Options {
    let mut options = Options::default();
    options.base.base.disable_file_header = Some(true);
    options.base.base.theme = Some(ThemeOption::Themes(crate::constants::default_themes()));
    options.base.base.theme_type = Some(ThemeTypes::Light);
    options
}

#[test]
fn theme_type_updates_file_diff_set_theme_type_applies_paired_theme_type_changes_immediately() {
    let dom = Rc::new(FakeDom::default());
    let container = dom.doc.create_element("div");
    let instance = instance(&dom, paired_theme_options());
    render_deferred(
        &instance,
        container,
        &parse(
            file("example.ts", "const value = 1;\n"),
            file("example.ts", "const value = 2;\n"),
        ),
    );

    let style = theme_style(&dom, container);
    assert_color_scheme(&dom, style, "light", "dark");

    instance.set_theme_type(ThemeTypes::Dark);
    assert_color_scheme(&dom, style, "dark", "light");

    instance.set_theme_type(ThemeTypes::Light);
    assert_color_scheme(&dom, style, "light", "dark");
    instance.lifecycle().clean_up(false);
}

#[test]
fn theme_type_updates_file_diff_render_applies_theme_type_changes_during_partial_renders() {
    let dom = Rc::new(FakeDom::default());
    let container = dom.doc.create_element("div");
    let file_diff = parse(
        make_long_file("example.ts", 8),
        make_long_file("example.ts", 9),
    );
    let instance = instance(&dom, paired_theme_options());
    let render = |range: Option<RenderRange>| {
        instance.lifecycle().render(FileDiffRenderProps {
            file_container: Some(container),
            file_diff: Some(Rc::clone(&file_diff)),
            render_range: range,
            defer_managers: true,
            prevent_emit: true,
            ..FileDiffRenderProps::default()
        });
    };

    render(window(0.0, 8.0));
    let style = theme_style(&dom, container);
    assert_color_scheme(&dom, style, "light", "dark");

    let mut options = instance.options().clone();
    options.base.base.theme_type = Some(ThemeTypes::Dark);
    instance.set_options(Some(options));
    render(window(4.0, 8.0));

    assert_eq!(theme_style(&dom, container), style);
    assert_color_scheme(&dom, style, "dark", "light");
    instance.lifecycle().clean_up(false);
}
