//! Transport values shared by guests, engines, and renderers, plus the host-internal seam through
//! which an engine commits batches into a renderer-owned surface.
use std::rc::Rc;

pub type NodeId = u32;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PresentationError {
    #[error("presentation surface or mount point has been revoked")]
    Revoked,
    #[error("unsupported presentation operation: {0}")]
    Unsupported(String),
    #[error("invalid presentation mutation batch: {0}")]
    InvalidBatch(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    Text(String),
    Float(f64),
    Int(i64),
    Boolean(bool),
    None,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Mutation<C = ()> {
    PushId(NodeId),
    SetId(NodeId),
    Child(u32),
    Pop,
    CreateElement((String, Option<String>)),
    CreateText(String),
    MountComponent(C),
    CloneNode,
    AppendChildren(u32),
    ReplaceWith(u32),
    InsertAfter(u32),
    InsertBefore(u32),
    SetAttribute((String, Option<String>, AttributeValue)),
    SetText(String),
    AddEventListener(String),
    RemoveEventListener(String),
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub revision: u64,
    pub element: NodeId,
    pub name: String,
    pub bubbles: bool,
    pub data_json: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventResponse {
    pub prevent_default: bool,
    pub stop_propagation: bool,
}

pub type SurfaceEventHandler = Rc<dyn Fn(Event) -> EventResponse>;

pub trait SurfaceSink<C = ()>: Send + Sync {
    fn is_live(&self) -> bool;
    fn apply(&self, edits: &[Mutation<C>]) -> Result<u64, PresentationError>;
    fn withdraw(&self);
}

pub struct SurfaceSession {
    dispatch: SurfaceEventHandler,
    on_drop: Option<Box<dyn FnOnce()>>,
}

impl SurfaceSession {
    pub fn new(dispatch: impl Fn(Event) -> EventResponse + 'static) -> Self {
        Self {
            dispatch: Rc::new(dispatch),
            on_drop: None,
        }
    }

    pub fn with_drop(
        dispatch: impl Fn(Event) -> EventResponse + 'static,
        on_drop: impl FnOnce() + 'static,
    ) -> Self {
        Self {
            dispatch: Rc::new(dispatch),
            on_drop: Some(Box::new(on_drop)),
        }
    }

    pub fn handler(&self) -> SurfaceEventHandler {
        self.dispatch.clone()
    }
}

impl Drop for SurfaceSession {
    fn drop(&mut self) {
        if let Some(on_drop) = self.on_drop.take() {
            on_drop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    // ── session lifecycle: the drop hook is the renderer's withdrawal signal; it fires
    //    exactly once, and only for sessions created with one ──────────────────────────────

    #[test]
    fn session_drop_fires_the_hook_exactly_once() {
        let drops = Rc::new(Cell::new(0_u32));
        let counted = drops.clone();
        let session = SurfaceSession::with_drop(
            |_| EventResponse::default(),
            move || {
                counted.set(counted.get() + 1);
            },
        );
        let handler = session.handler();
        let response = handler(Event {
            revision: 7,
            element: 3,
            name: "click".into(),
            bubbles: true,
            data_json: "null".into(),
        });
        assert_eq!(response, EventResponse::default());
        assert_eq!(drops.get(), 0);
        drop(session);
        assert_eq!(drops.get(), 1);
        // The handler outlives its session; dispatch must stay usable after the hook fired.
        let _ = handler(Event {
            revision: 8,
            element: 3,
            name: "click".into(),
            bubbles: true,
            data_json: "null".into(),
        });
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn session_without_hook_drops_quietly() {
        let session = SurfaceSession::new(|_| EventResponse {
            prevent_default: true,
            stop_propagation: false,
        });
        let handler = session.handler();
        drop(session);
        assert_eq!(
            handler(Event {
                revision: 0,
                element: 0,
                name: "mounted".into(),
                bubbles: false,
                data_json: "null".into(),
            }),
            EventResponse {
                prevent_default: true,
                stop_propagation: false,
            }
        );
    }
}
