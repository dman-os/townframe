// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/managers/UniversalRenderingManager.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: upstream keeps `callbacks` and `frameId` as module-level state and
// calls the `requestAnimationFrame` global; here they live in a
// `UniversalRenderingManager` value (a cheap clone shares the same queue, as
// the module singleton is shared) and the frame source is the
// `AnimationFrames` trait, so the queue runs against a browser, a test clock
// or any other host. `queueRender(cb)` / `dequeueRender(cb)` /
// `clearRenderQueue()` are the methods `queue_render` / `dequeue_render` /
// `clear_render_queue`. A callback's identity (JavaScript `Set` membership of
// the function object) is the address of its `Rc`. Upstream wraps each
// callback in try/catch and `console.error`s; a panicking callback here
// propagates (errors in a render callback are invariant breaks, not recoverable
// input errors). The `Set` iterations keep JavaScript's live semantics through
// `InsertionMap`.

use std::cell::RefCell;
use std::rc::Rc;

use crate::js_collections::InsertionMap;

/// `(time) => unknown`; identity is the `Rc` allocation.
pub type RenderCallback = Rc<dyn Fn(f64)>;

pub type FrameId = u32;

/// `requestAnimationFrame` / `cancelAnimationFrame`.
pub trait AnimationFrames {
    fn request_animation_frame(&self, callback: Box<dyn FnOnce(f64)>) -> FrameId;
    fn cancel_animation_frame(&self, frame_id: FrameId);
}

fn callback_key(callback: &RenderCallback) -> usize {
    Rc::as_ptr(callback).cast::<()>() as usize
}

struct QueueState {
    callbacks: InsertionMap<usize, RenderCallback>,
    frame_id: Option<FrameId>,
}

pub struct UniversalRenderingManager<Frames: AnimationFrames> {
    frames: Rc<Frames>,
    state: Rc<RefCell<QueueState>>,
}

impl<Frames: AnimationFrames> Clone for UniversalRenderingManager<Frames> {
    fn clone(&self) -> Self {
        Self {
            frames: Rc::clone(&self.frames),
            state: Rc::clone(&self.state),
        }
    }
}

impl<Frames: AnimationFrames + 'static> UniversalRenderingManager<Frames> {
    pub fn new(frames: Rc<Frames>) -> Self {
        Self {
            frames,
            state: Rc::new(RefCell::new(QueueState {
                callbacks: InsertionMap::default(),
                frame_id: None,
            })),
        }
    }

    pub fn queue_render(&self, callback: &RenderCallback) {
        let needs_frame = {
            let mut state = self.state.borrow_mut();
            state
                .callbacks
                .insert(callback_key(callback), Rc::clone(callback));
            state.frame_id.is_none()
        };
        // `frameId ??= requestAnimationFrame(render)`; the borrow is released
        // first because a host may run the callback synchronously.
        if needs_frame {
            let frame_id = self.request_render_frame();
            self.state.borrow_mut().frame_id = Some(frame_id);
        }
    }

    pub fn dequeue_render(&self, callback: &RenderCallback) {
        let cancel = {
            let mut state = self.state.borrow_mut();
            if state.callbacks.remove(&callback_key(callback)) && state.callbacks.is_empty() {
                state.frame_id.take()
            } else {
                None
            }
        };
        if let Some(frame_id) = cancel {
            self.frames.cancel_animation_frame(frame_id);
        }
    }

    // Drops every queued callback and cancels the pending frame.
    pub fn clear_render_queue(&self) {
        let cancel = {
            let mut state = self.state.borrow_mut();
            state.callbacks.clear();
            state.frame_id.take()
        };
        if let Some(frame_id) = cancel {
            self.frames.cancel_animation_frame(frame_id);
        }
    }

    fn request_render_frame(&self) -> FrameId {
        let manager = self.clone();
        self.frames
            .request_animation_frame(Box::new(move |time| manager.render(time)))
    }

    fn render(&self, time: f64) {
        let to_iterate = {
            let mut state = self.state.borrow_mut();
            let to_iterate = state.callbacks.entries();
            // The frame id stays set while callbacks run, so a callback that
            // requeues does not request a second frame; the follow-up below
            // does.
            state.callbacks.clear();
            to_iterate
        };
        for (_, callback) in to_iterate {
            callback(time);
        }
        // If render picked up any new callbacks, lets trigger a new
        // requestAnimationFrame
        let has_new = !self.state.borrow().callbacks.is_empty();
        if has_new {
            let frame_id = self.request_render_frame();
            self.state.borrow_mut().frame_id = Some(frame_id);
        } else {
            self.state.borrow_mut().frame_id = None;
        }
    }
}
