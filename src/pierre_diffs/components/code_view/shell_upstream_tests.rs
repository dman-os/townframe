// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/CodeView.pointerEvents.test.ts
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; file items become diff items (divergences.psv).

//! Upstream's `test/CodeView.pointerEvents.test.ts` (C4), ported with its names
//! (prefixed by its `describe`). Each case runs `setup(root)` on an empty
//! viewer, so the immediate `render` `setup` ends with runs the real render
//! pass over no items. `await wait(150)` is `advance_clock(150.0)` on the fake clock;
//! the mobile Safari case builds its viewer over a host whose `PlatformInfo`
//! says so, where upstream re-imports the module under a cache-busting query.
//! Below them, the shell's own arms that no upstream case reaches (the
//! `headerFooter` and `elementPooling` cases need the render pass: C5).
//! Test-only.

use std::rc::Rc;

use super::CodeView;
use super::options::CodeViewOptions;
use crate::document::NodeKey;
use crate::fake_interaction_dom::FakeDom;
use crate::highlighter_types::PlainTextHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;

pub(super) type View = CodeView<FakeDom, u8, u8>;

pub(super) fn viewer_with(dom: &Rc<FakeDom>, options: CodeViewOptions<u8, u8>) -> Rc<View> {
    CodeView::new(
        Rc::clone(dom),
        ResizeObserverHub::new(Rc::clone(dom)),
        UniversalRenderingManager::new(Rc::clone(dom)),
        Rc::new(PlainTextHighlighter),
        options,
    )
}

fn viewer(options: CodeViewOptions<u8, u8>) -> (Rc<FakeDom>, Rc<View>) {
    let dom = Rc::new(FakeDom::default());
    let view = viewer_with(&dom, options);
    (dom, view)
}

/// `getPointerEventsTarget(root)`: `root.firstElementChild.lastElementChild`.
fn pointer_events_target(dom: &FakeDom, root: NodeKey) -> NodeKey {
    let container = dom.doc.children(root)[0];
    *dom.doc
        .children(container)
        .last()
        .expect("the sticky container")
}

fn code_overflow_block(dom: &FakeDom, target: NodeKey) -> String {
    dom.doc
        .get_style_property(target, "--diffs-overflow-override")
}

fn pointer_events(dom: &FakeDom, target: NodeKey) -> String {
    dom.doc.get_style_property(target, "pointer-events")
}

#[test]
fn code_view_pointer_events_while_scrolling_disables_pointer_events_by_default_during_scroll_and_restores_after_delay()
 {
    let (dom, view) = viewer(CodeViewOptions::default());
    let root = dom.doc.create_element("div");
    view.setup(root);
    let target = pointer_events_target(&dom, root);

    view.handle_scroll();

    assert_eq!(pointer_events(&dom, target), "none");
    assert_eq!(code_overflow_block(&dom, target), "");
    dom.advance_clock(150.0);
    assert_eq!(pointer_events(&dom, target), "");
    assert_eq!(code_overflow_block(&dom, target), "");
    view.clean_up();
}

#[test]
fn code_view_pointer_events_while_scrolling_keeps_pointer_events_enabled_when_opted_out() {
    let (dom, view) = viewer(CodeViewOptions {
        pointer_events_on_scroll: Some(true),
        ..CodeViewOptions::default()
    });
    let root = dom.doc.create_element("div");
    view.setup(root);
    let target = pointer_events_target(&dom, root);

    view.handle_scroll();

    assert_eq!(pointer_events(&dom, target), "");
    assert_eq!(code_overflow_block(&dom, target), "");
    dom.advance_clock(150.0);
    assert_eq!(code_overflow_block(&dom, target), "");
    view.clean_up();
}

#[test]
fn code_view_pointer_events_while_scrolling_clean_up_restores_pointer_events_immediately() {
    let (dom, view) = viewer(CodeViewOptions::default());
    let root = dom.doc.create_element("div");
    view.setup(root);
    let target = pointer_events_target(&dom, root);

    view.handle_scroll();
    assert_eq!(pointer_events(&dom, target), "none");
    assert_eq!(code_overflow_block(&dom, target), "");

    view.clean_up();

    assert_eq!(pointer_events(&dom, target), "");
    assert_eq!(code_overflow_block(&dom, target), "");
}

#[test]
fn code_view_pointer_events_while_scrolling_clean_up_unsets_the_root_overflow_anchor_style() {
    let (dom, view) = viewer(CodeViewOptions::default());
    let root = dom.doc.create_element("div");
    dom.doc.set_style_property(root, "overflow-anchor", "auto");

    view.setup(root);
    assert_eq!(dom.doc.get_style_property(root, "overflow-anchor"), "none");

    view.clean_up();

    assert_eq!(dom.doc.get_style_property(root, "overflow-anchor"), "");
}

#[test]
fn code_view_pointer_events_while_scrolling_pointer_events_are_restored_after_opting_out_mid_scroll()
 {
    let (dom, view) = viewer(CodeViewOptions::default());
    let root = dom.doc.create_element("div");
    view.setup(root);
    let target = pointer_events_target(&dom, root);

    view.handle_scroll();
    assert_eq!(pointer_events(&dom, target), "none");
    assert_eq!(code_overflow_block(&dom, target), "");

    view.set_options(Some(CodeViewOptions {
        pointer_events_on_scroll: Some(true),
        ..CodeViewOptions::default()
    }));

    dom.advance_clock(150.0);
    assert_eq!(pointer_events(&dom, target), "");
    assert_eq!(code_overflow_block(&dom, target), "");
    view.clean_up();
}

#[test]
fn code_view_pointer_events_while_scrolling_applies_overflow_override_while_scrolling_on_mobile_safari_only()
 {
    let dom = Rc::new(FakeDom::default());
    {
        let mut info = dom.platform_info.borrow_mut();
        info.max_touch_points = 5;
        info.platform = Some("iPhone".to_owned());
        info.user_agent = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1".to_owned();
    }
    let view = viewer_with(&dom, CodeViewOptions::default());
    let root = dom.doc.create_element("div");
    view.setup(root);
    let target = pointer_events_target(&dom, root);

    view.handle_scroll();

    assert_eq!(pointer_events(&dom, target), "none");
    assert_eq!(code_overflow_block(&dom, target), "hidden");
    dom.advance_clock(150.0);
    assert_eq!(pointer_events(&dom, target), "");
    assert_eq!(code_overflow_block(&dom, target), "auto");
    view.clean_up();
}
