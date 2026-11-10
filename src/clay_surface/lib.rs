//! Engine-neutral presentation wire, `WriteMutations` replay, and retained surface tree.
//!
//! The WIT `presentation` contract ships ordered batches of [`Mutation`] operations that mirror
//! `dioxus_core::WriteMutations` one-to-one. A renderer backend is any `WriteMutations`
//! implementation plus event translation:
//! - Direct Blitz documents replay batches into `dioxus_native_dom::DioxusState::writer`.
//! - Host-composed surfaces replay batches into [`SurfaceTree`], which renders into the host
//!   Dioxus `VirtualDom` through `ComponentMount` without `ElementId` collisions
//!   and withdraws its subtree when its host `Mount` or `Instance` closes.
//!
//! Modules: `wire` holds the transport values and the host sink/session seam, `replay` drives any
//! `WriteMutations` writer from a batch, `tree` validates batches into the retained arena, and
//! `render` projects that arena into a host `VirtualDom` and owns the static tag, attribute, and
//! listener vocabulary a batch may name.
/// Spike instrumentation (W1): a hook the host installs to timestamp stages of event delivery.
pub mod probe {
    use std::cell::Cell;
    thread_local! { static HOOK: Cell<Option<fn(&'static str)>> = const { Cell::new(None) }; }
    pub fn install(hook: fn(&'static str)) { HOOK.set(Some(hook)); }
    pub fn mark(label: &'static str) { if let Some(h) = HOOK.get() { h(label) } }
}
mod render;
mod replay;
mod tree;
mod wire;

pub use render::SurfaceProjection;
pub use render::SurfaceRoot;
pub use replay::replay;
pub use tree::SurfaceTree;
pub use wire::AttributeValue;
pub use wire::ELEMENT_REPLY;
pub use wire::ElementCommand;
pub use wire::ElementHandles;
pub use wire::ElementRead;
pub use wire::Event;
pub use wire::EventResponse;
pub use wire::Mutation;
pub use wire::NodeId;
pub use wire::PresentationError;
pub use wire::ScrollBehavior;
pub use wire::ScrollOffsets;
pub use wire::SurfaceEventHandler;
pub use wire::SurfaceSession;
pub use wire::SurfaceSink;
