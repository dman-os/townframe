// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/Virtualizer.scrollState.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; jsdom's root,
// `scrollTo` stubs and `window.scrollY` / `innerHeight` / `scrollHeight`
// overrides are the `FakeHost` world (its `scrollTo` writes the scroll
// position and records the call, as the stubs do; unlike the jsdom stub it
// clamps to the scrollable range as a browser does, which none of the
// assertions can distinguish: every target is already inside the range or
// clamped by the virtualizer first); `await wait(0)` is flushing the fake
// frame queue; `toEqual` on the recorded scroll calls compares the trace.

use std::rc::Rc;

use serde_json::json;

use crate::components::virtualizer::Root;
use crate::components::virtualizer::ScrollBehavior;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerConfig;
use crate::components::virtualizer::VirtualizerScrollOptions;
use crate::fake_dom::ElementPatch;
use crate::fake_dom::FakeHost;
use crate::fake_dom::ROOT;
use crate::fake_dom::WorldPatch;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;

fn last_scroll_call(host: &FakeHost) -> serde_json::Value {
    host.take_trace()
        .into_iter()
        .rev()
        .find(|entry| entry[0] == "window.scrollTo" || entry[0] == "element.scrollTo")
        .expect("a scroll call was recorded")
}

fn virtualizer_over(host: &Rc<FakeHost>) -> Rc<Virtualizer<FakeHost>> {
    Virtualizer::new(
        Rc::clone(host),
        UniversalRenderingManager::new(Rc::clone(host)),
        VirtualizerConfig::default(),
    )
}

#[test]
fn reads_and_restores_an_element_root_logical_scroll_position() {
    let host = Rc::new(FakeHost::new(false));
    host.apply_world(&WorldPatch {
        scroll_top: Some(1_200.0),
        viewport_height: Some(800.0),
        scroll_height: Some(5_000.0),
        elements: [("content".to_owned(), ElementPatch::default())].into(),
        ..WorldPatch::default()
    });
    let virtualizer = virtualizer_over(&host);

    virtualizer.setup(
        Root::Element(host.element(ROOT)),
        Some(host.element("content")),
    );
    host.frames.flush_frames(10, 0.0);
    assert_eq!(virtualizer.get_scroll_top(), 1_200.0);

    virtualizer.scroll_to(VirtualizerScrollOptions {
        top: 2_500.0,
        behavior: ScrollBehavior::Instant,
    });
    assert_eq!(virtualizer.get_scroll_top(), 2_500.0);
    assert_eq!(
        last_scroll_call(&host),
        json!(["element.scrollTo", ROOT, 2_500.0, "instant"])
    );
    host.frames.flush_frames(10, 0.0);
    assert_eq!(virtualizer.get_window_specs().top, 1_500.0);

    virtualizer.scroll_to(VirtualizerScrollOptions {
        top: 10_000.0,
        behavior: ScrollBehavior::Auto,
    });
    assert_eq!(virtualizer.get_scroll_top(), 4_200.0);
    assert_eq!(
        last_scroll_call(&host),
        json!(["element.scrollTo", ROOT, 4_200.0, "auto"])
    );
    virtualizer.scroll_to(VirtualizerScrollOptions {
        top: -100.0,
        behavior: ScrollBehavior::Auto,
    });
    assert_eq!(virtualizer.get_scroll_top(), 0.0);

    virtualizer.clean_up();
}

#[test]
fn reads_and_restores_a_document_root_logical_scroll_position() {
    let host = Rc::new(FakeHost::new(true));
    host.apply_world(&WorldPatch {
        scroll_top: Some(1_200.0),
        viewport_height: Some(800.0),
        scroll_height: Some(5_000.0),
        ..WorldPatch::default()
    });
    let virtualizer = virtualizer_over(&host);

    virtualizer.setup(Root::Document, None);
    host.frames.flush_frames(10, 0.0);
    assert_eq!(virtualizer.get_scroll_top(), 1_200.0);

    virtualizer.scroll_to(VirtualizerScrollOptions {
        top: 2_500.0,
        behavior: ScrollBehavior::Instant,
    });
    assert_eq!(virtualizer.get_scroll_top(), 2_500.0);
    assert_eq!(
        last_scroll_call(&host),
        json!(["window.scrollTo", 2_500.0, "instant"])
    );
    host.frames.flush_frames(10, 0.0);
    assert_eq!(virtualizer.get_window_specs().top, 1_500.0);

    virtualizer.scroll_to(VirtualizerScrollOptions {
        top: 10_000.0,
        behavior: ScrollBehavior::Auto,
    });
    assert_eq!(virtualizer.get_scroll_top(), 4_200.0);
    assert_eq!(
        last_scroll_call(&host),
        json!(["window.scrollTo", 4_200.0, "auto"])
    );
    virtualizer.scroll_to(VirtualizerScrollOptions {
        top: -100.0,
        behavior: ScrollBehavior::Auto,
    });
    assert_eq!(virtualizer.get_scroll_top(), 0.0);

    virtualizer.clean_up();
}
