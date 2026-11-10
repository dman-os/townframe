// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/UniversalRenderingManager.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: bun:test to nextest; test titles are snake_case; bun's `mock()`
// callbacks are `Rc<RefCell<Vec<f64>>>` call logs (the times each call
// received); the module-level queue is a `UniversalRenderingManager` over the
// `FakeFrames` harness.

use std::cell::RefCell;
use std::rc::Rc;

use crate::fake_dom::FakeFrames;
use crate::managers::universal_rendering_manager::RenderCallback;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;

fn harness() -> (Rc<FakeFrames>, UniversalRenderingManager<FakeFrames>) {
    let frames = Rc::new(FakeFrames::default());
    let manager = UniversalRenderingManager::new(Rc::clone(&frames));
    (frames, manager)
}

fn mock() -> (RenderCallback, Rc<RefCell<Vec<f64>>>) {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let log = Rc::clone(&calls);
    let callback: RenderCallback = Rc::new(move |time| log.borrow_mut().push(time));
    (callback, calls)
}

#[test]
fn queues_the_same_callback_once_per_frame() {
    let (frames, manager) = harness();
    let (callback, calls) = mock();

    manager.queue_render(&callback);
    manager.queue_render(&callback);

    assert_eq!(frames.frame_ids(), vec![1]);
    frames.run_frame(1, 42.0);

    assert_eq!(*calls.borrow(), vec![42.0]);
    assert!(frames.frame_ids().is_empty());

    manager.queue_render(&callback);
    assert_eq!(frames.frame_ids(), vec![2]);

    frames.run_frame(2, 84.0);
    assert_eq!(*calls.borrow(), vec![42.0, 84.0]);
}

#[test]
fn dequeues_the_same_callback_idempotently() {
    let (frames, manager) = harness();
    let (callback, calls) = mock();

    manager.queue_render(&callback);
    manager.dequeue_render(&callback);
    manager.dequeue_render(&callback);

    assert!(calls.borrow().is_empty());
    assert_eq!(frames.cancelled_frame_ids(), vec![1]);
    assert!(frames.frame_ids().is_empty());
}

#[test]
fn keeps_the_shared_frame_when_dequeuing_one_callback() {
    let (frames, manager) = harness();
    let (dequeued_callback, dequeued_calls) = mock();
    let (queued_callback, queued_calls) = mock();

    manager.queue_render(&dequeued_callback);
    manager.queue_render(&queued_callback);
    manager.dequeue_render(&dequeued_callback);
    manager.dequeue_render(&dequeued_callback);

    assert!(frames.cancelled_frame_ids().is_empty());
    assert_eq!(frames.frame_ids(), vec![1]);

    frames.run_frame(1, 0.0);
    assert!(dequeued_calls.borrow().is_empty());
    assert_eq!(queued_calls.borrow().len(), 1);
}

#[test]
fn ignores_dequeues_for_callbacks_that_are_not_queued() {
    let (frames, manager) = harness();
    let (queued_callback, queued_calls) = mock();
    let (unknown_callback, unknown_calls) = mock();

    manager.queue_render(&queued_callback);
    manager.dequeue_render(&unknown_callback);

    assert!(frames.cancelled_frame_ids().is_empty());
    assert_eq!(frames.frame_ids(), vec![1]);

    frames.run_frame(1, 0.0);
    assert_eq!(queued_calls.borrow().len(), 1);
    assert!(unknown_calls.borrow().is_empty());
}

#[test]
fn queues_a_new_frame_after_the_previous_frame_is_cancelled() {
    let (frames, manager) = harness();
    let (cancelled_callback, cancelled_calls) = mock();
    let (replacement_callback, replacement_calls) = mock();

    manager.queue_render(&cancelled_callback);
    manager.dequeue_render(&cancelled_callback);
    manager.queue_render(&replacement_callback);

    assert_eq!(frames.cancelled_frame_ids(), vec![1]);
    assert_eq!(frames.frame_ids(), vec![2]);

    frames.run_frame(2, 0.0);
    assert!(cancelled_calls.borrow().is_empty());
    assert_eq!(replacement_calls.borrow().len(), 1);
}

#[test]
fn does_not_cancel_the_active_frame_when_callbacks_requeue_during_render() {
    let (frames, manager) = harness();
    let (stale_callback, stale_calls) = mock();
    let (replacement_callback, replacement_calls) = mock();

    let requeuing: RenderCallback = {
        let manager = manager.clone();
        let stale_callback = Rc::clone(&stale_callback);
        let replacement_callback = Rc::clone(&replacement_callback);
        Rc::new(move |_time| {
            manager.dequeue_render(&stale_callback);
            manager.queue_render(&replacement_callback);
        })
    };
    manager.queue_render(&requeuing);
    manager.queue_render(&stale_callback);

    assert_eq!(frames.frame_ids(), vec![1]);
    frames.run_frame(1, 0.0);

    // The stale callback was already copied into this pass, while its
    // replacement belongs to exactly one follow-up frame.
    assert_eq!(stale_calls.borrow().len(), 1);
    assert!(frames.cancelled_frame_ids().is_empty());
    assert_eq!(frames.frame_ids(), vec![2]);

    frames.run_frame(2, 0.0);
    assert_eq!(replacement_calls.borrow().len(), 1);
    assert!(frames.frame_ids().is_empty());
}

// `clearRenderQueue` has no upstream test; this pins its contract (drop every
// callback, cancel the pending frame, allow a fresh frame afterwards).
#[test]
fn clear_render_queue_drops_callbacks_and_cancels_the_frame() {
    let (frames, manager) = harness();
    let (callback, calls) = mock();

    manager.queue_render(&callback);
    manager.clear_render_queue();
    assert_eq!(frames.cancelled_frame_ids(), vec![1]);
    assert!(frames.frame_ids().is_empty());

    manager.queue_render(&callback);
    assert_eq!(frames.frame_ids(), vec![2]);
    frames.run_frame(2, 5.0);
    assert_eq!(*calls.borrow(), vec![5.0]);
}
