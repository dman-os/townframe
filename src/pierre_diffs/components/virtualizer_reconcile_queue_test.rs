// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/Virtualizer.reconcileQueue.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; jsdom's root and
// container are `FakeHost` elements; `await wait(20)` is flushing the fake
// frame queue (upstream waits out real animation frames).

use std::cell::Cell;
use std::rc::Rc;

use crate::components::virtualizer::InstanceRef;
use crate::components::virtualizer::Root;
use crate::components::virtualizer::SubscribedInstance;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerConfig;
use crate::fake_dom::ElementPatch;
use crate::fake_dom::FakeHost;
use crate::fake_dom::ROOT;
use crate::fake_dom::WorldPatch;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;

struct CountingInstance {
    reconciles: Cell<usize>,
}

impl SubscribedInstance for CountingInstance {
    // Never repaints inside a virtualizer pass, mirroring an instance whose
    // content landed via a direct render() call.
    fn on_render(&self, _dirty: bool) -> bool {
        false
    }

    fn reconcile_heights(&self) -> bool {
        self.reconciles.set(self.reconciles.get() + 1);
        true
    }

    fn set_visibility(&self, _visible: bool) {}
}

// The virtualizer only auto-reconciles heights for instances whose onRender
// repainted inside one of its own passes. Content applied from other paths
// (host/React render calls, async highlight completions) must be able to
// queue a reconciliation, or measured line deltas cleared by a layout reset
// (e.g. edit mode flipping expandUnchanged) never get re-captured and the
// instance's placeholder renders at its baseline estimate.
#[test]
fn request_height_reconcile_reconciles_instances_the_pass_did_not_repaint() {
    let host = Rc::new(FakeHost::new(false));
    host.apply_world(&WorldPatch {
        viewport_height: Some(800.0),
        scroll_height: Some(800.0),
        elements: [
            ("content".to_owned(), ElementPatch::default()),
            ("container".to_owned(), ElementPatch::default()),
        ]
        .into(),
        ..WorldPatch::default()
    });
    let instance = Rc::new(CountingInstance {
        reconciles: Cell::new(0),
    });
    let instance_ref: InstanceRef = Rc::<CountingInstance>::clone(&instance);

    let virtualizer = Virtualizer::new(
        Rc::clone(&host),
        UniversalRenderingManager::new(Rc::clone(&host)),
        VirtualizerConfig::default(),
    );
    virtualizer.setup(
        Root::Element(host.element(ROOT)),
        Some(host.element("content")),
    );
    let _disconnect = virtualizer.connect(host.element("container"), Rc::clone(&instance_ref));
    host.frames.flush_frames(20, 0.0);

    // onRender returned false in the connect pass, so nothing reconciled.
    let baseline = instance.reconciles.get();

    virtualizer.request_height_reconcile(&instance_ref);
    host.frames.flush_frames(20, 0.0);
    assert_eq!(instance.reconciles.get(), baseline + 1);

    // The queue drains once satisfied: further idle passes don't re-measure.
    host.frames.flush_frames(20, 0.0);
    assert_eq!(instance.reconciles.get(), baseline + 1);

    virtualizer.clean_up();
}
