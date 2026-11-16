//! The advanced (CodeView) arms of `VirtualizedFileDiff` that no upstream test
//! and no oracle scenario drives, because they need a connected container: the
//! upstream tests read `render` and `cleanUp(true)` through CodeView itself
//! (C5). Each test names the upstream arm it pins. Test-only, original to this
//! crate.

use std::rc::Rc;

use super::advanced_upstream_tests::StubCodeView;
use super::advanced_upstream_tests::advanced_world;
use super::layout_upstream_tests::parse_files;
use super::layout_upstream_tests::plain_file;
use super::window_upstream_tests::Options;
use super::window_upstream_tests::World;
use crate::components::file_diff::FileDiffRenderProps;
use crate::components::file_diff_hydration_upstream_tests::panic_message;
use crate::components::virtualizer::SubscribedInstance;
use crate::types::layout::VirtualWindowSpecs;
use crate::types::options::ThemeTypes;
use crate::types::patch::FileDiffMetadata;

fn numbered(count: usize, edited: usize) -> String {
    (1..=count)
        .map(|line| {
            if line == edited {
                format!("edited {line}\n")
            } else {
                format!("line {line}\n")
            }
        })
        .collect()
}

fn long_diff(edited: usize) -> Rc<FileDiffMetadata> {
    parse_files(
        plain_file("long.txt", &numbered(400, 0)),
        plain_file("long.txt", &numbered(400, edited)),
    )
}

/// Every line changed: one change block longer than a render window, so the
/// render range has rows below the window (a lone edit renders as one short hunk).
fn all_lines_changed() -> Rc<FileDiffMetadata> {
    parse_files(
        plain_file("long.txt", &numbered(400, 0)),
        plain_file(
            "long.txt",
            &(1..=400)
                .map(|line| format!("edited {line}\n"))
                .collect::<String>(),
        ),
    )
}

fn render(world: &World, container: crate::document::NodeKey, diff: &Rc<FileDiffMetadata>) {
    world.instance.render(FileDiffRenderProps {
        file_container: Some(container),
        file_diff: Some(Rc::clone(diff)),
        defer_managers: true,
        prevent_emit: true,
        ..FileDiffRenderProps::default()
    });
}

fn advanced_with_window(top: f64, bottom: f64) -> (World, Rc<StubCodeView>) {
    let (world, code_view) = advanced_world(Options::default());
    code_view.window.set(VirtualWindowSpecs { top, bottom });
    (world, code_view)
}

#[test]
fn advanced_render_is_visible_without_connecting_and_has_no_buffers() {
    // `render`: `isAdvancedMode()` sets `isVisible` and skips `connect`;
    // `shouldDisableVirtualizationBuffers` is true, so the render range's
    // buffers are not applied (the sticky container flows items contiguously).
    let (world, _code_view) = advanced_with_window(0.0, 100.0);
    let container = world.dom.doc.create_element("div");
    let diff = all_lines_changed();
    world
        .instance
        .update_code_view_layout(&diff, 0.0, None, None);

    render(&world, container, &diff);

    assert!(world.instance.is_setup());
    assert!(world.instance.is_visible());
    let render_range = world
        .instance
        .file_diff()
        .virtualized()
        .render_range()
        .expect("a render range");
    assert!(render_range.buffer_after > 0.0);
    let shadow_root = world.dom.doc.shadow_root(container).expect("a shadow root");
    assert!(
        world
            .dom
            .doc
            .query_by_attribute(shadow_root, "data-virtualizer-buffer", None)
            .is_empty()
    );
    world.instance.clean_up(false);
}

#[test]
fn advanced_set_visibility_does_nothing() {
    let (world, _code_view) = advanced_with_window(0.0, 1_000.0);
    let container = world.dom.doc.create_element("div");
    let diff = long_diff(5);
    render(&world, container, &diff);
    assert!(world.instance.is_visible());

    SubscribedInstance::set_visibility(&*world.instance, false);

    assert!(world.instance.is_visible());
    world.instance.clean_up(false);
}

#[test]
fn advanced_clean_up_recycle_recomputes_the_layout_when_the_rendered_diff_lags_the_working_diff() {
    let (world, code_view) = advanced_with_window(0.0, 1_000.0);
    let container = world.dom.doc.create_element("div");
    let first = long_diff(5);
    render(&world, container, &first);

    // The rendered diff is the working diff: nothing to recompute.
    world.instance.clean_up(true);
    assert!(code_view.instance_changed_calls.borrow().is_empty());

    // `cleanUp` disabled the instance; CodeView calls `virtualizedSetup()`
    // before it renders a recycled item (CodeView.ts:3620).
    world.instance.file_diff().virtualized().virtualized_setup();
    render(&world, container, &first);
    world
        .instance
        .file_diff()
        .virtualized()
        .update_input_diff(long_diff(6), None);
    world.instance.clean_up(true);
    assert_eq!(*code_view.instance_changed_calls.borrow(), [true]);

    world.instance.clean_up(false);
}

#[test]
fn advanced_top_is_the_local_top_the_code_view_assigns() {
    let (world, code_view) = advanced_with_window(0.0, 1_000.0);
    code_view.local_top.set(321.5);

    world.instance.sync_virtualized_top();

    assert_eq!(world.instance.top(), Some(321.5));
}

#[test]
fn advanced_options_and_theme_type_belong_to_the_code_view() {
    let (world, _code_view) = advanced_with_window(0.0, 1_000.0);

    let message = panic_message(|| world.instance.set_options(Some(Options::default())));
    assert!(message.contains(
        "VirtualizedFileDiff.setOptions cannot be used inside CodeView. Update CodeView options instead."
    ));
    // The throw comes before the `options == null` return.
    let message = panic_message(|| world.instance.set_options(None));
    assert!(message.contains("cannot be used inside CodeView"));
    let message = panic_message(|| world.instance.set_theme_type(ThemeTypes::Dark));
    assert!(message.contains(
        "VirtualizedFileDiff.setThemeType cannot be used inside CodeView. Update CodeView options instead."
    ));
}
