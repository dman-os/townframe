//! The Micro Development Kit (ADR 015 §5): the single authoring API every micro uses.
//! `manifest` and `ComponentRef` are neutral vocabulary every backend compiles against;
//! `bindings` carries the WIT-generated surface; `mirror` is the typed document-mirror
//! authoring surface over it (typed decoding, availability/freshness evidence, named config
//! bindings); `native` is the in-process backend (authoring context, collections, provider
//! assembly); `guest` (feature `dioxus`) is the Dioxus VirtualDom adapter. The MDK adds
//! ergonomics, never authority: capabilities are still the host-issued resources.
mod manifest;
pub use manifest::{ConfigGrant, FacetAccess, MicroId, MicroManifest, Provision, ProvisionRef};

mod component;
pub use component::ComponentRef;

#[cfg(feature = "wit")]
pub mod bindings {
    wit_bindgen::generate!({
        path: "../clay_guest_session/wit",
        world: "micro-client",
        pub_export_macro: true,
    });
}

#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::{
    document_events, micro_lifecycle, presentation_events,
};
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::{documents, presentation};

#[cfg(feature = "wit")]
pub mod mirror;
#[cfg(feature = "wit")]
pub use mirror::{ConfigDocument, FacetState, MirrorSnapshot, MirrorWatch};

#[cfg(feature = "native")]
pub mod native;

#[cfg(feature = "dioxus")]
pub mod guest;
#[cfg(feature = "dioxus")]
pub use guest::GuestDom;

#[cfg(feature = "dioxus")]
pub mod wire;

#[cfg(all(test, feature = "dioxus", not(target_arch = "wasm32")))]
mod tests;
