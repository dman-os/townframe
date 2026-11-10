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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrollBehavior {
    Instant,
    Smooth,
}

/// Offsets of an element's own scrollable content, in CSS pixels.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScrollOffsets {
    pub left: f64,
    pub top: f64,
    pub behavior: ScrollBehavior,
}

/// A geometry value the host reads from a mounted element.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElementRead {
    ClientRect,
    ScrollOffset,
    ScrollSize,
}

/// The imperative half of Dioxus's `MountedData`, sent by a guest to one element it was told is
/// mounted. Fire-and-forget: a [`Self::Read`] is answered by an [`ELEMENT_REPLY`] event.
///
/// Ordering contract: commands a guest issues while handling one event are applied by the host
/// in the order issued, after that dispatch's mutations have been applied.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ElementCommand {
    Scroll(ScrollOffsets),
    ScrollBy(ScrollOffsets),
    SetFocus(bool),
    Read { request: u64, what: ElementRead },
}

/// The name of the event through which the host answers an [`ElementCommand::Read`]. It is not a
/// Dioxus event: the guest adapter consumes it before Dioxus sees it. `data_json` is
/// `{"request": id, "value": ...}` or `{"request": id, "error": "..."}`.
pub const ELEMENT_REPLY: &str = "element-reply";

/// The host's `MountedData` of every guest element that announced `mounted`, by guest element
/// id: what an [`ElementCommand`] acts on. It holds `Rc`s, so it stays on the renderer's
/// thread; the thread-safe side of a surface only tracks which ids are mounted.
#[derive(Clone, Default)]
pub struct ElementHandles(
    Rc<std::cell::RefCell<std::collections::BTreeMap<NodeId, Rc<dioxus_html::MountedData>>>>,
);

impl PartialEq for ElementHandles {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

impl ElementHandles {
    pub fn insert(&self, element: NodeId, mounted: dioxus_html::MountedData) {
        self.0.borrow_mut().insert(element, Rc::new(mounted));
    }

    pub fn get(&self, element: NodeId) -> Option<Rc<dioxus_html::MountedData>> {
        self.0.borrow().get(&element).cloned()
    }
}

pub trait SurfaceSink<C = ()>: Send + Sync {
    fn is_live(&self) -> bool;
    fn apply(&self, edits: &[Mutation<C>]) -> Result<u64, PresentationError>;
    /// Queue `command` for `element`. A sink with no renderer behind it cannot run one.
    fn element_command(
        &self,
        _element: NodeId,
        _command: ElementCommand,
    ) -> Result<(), PresentationError> {
        Err(PresentationError::Unsupported("element commands".into()))
    }
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
