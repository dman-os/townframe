//! The Micro Development Kit (ADR 015 §5): the single authoring API every micro uses.
//! `manifest` and `ComponentRef` are neutral vocabulary every backend compiles against;
//! `bindings` carries the WIT-generated surface; `mirror` is the typed document-mirror
//! authoring surface over it (typed decoding, availability/freshness evidence, named config
//! bindings); `native` (feature `in_process`) is the in-process backend (authoring context, collections, provider
//! assembly), renderer-agnostic; `view` is its Dioxus half (the slot component and collection
//! hook a host-linked view needs); `wasm` assembles Wasmtime-backed providers for a composition
//! root; `guest` (feature `dioxus`) is the guest backend — the same `activate(&Context)` shape
//! over the WIT imports, plus the Dioxus VirtualDom adapter and the `export_micro!` entry
//! point. The MDK adds ergonomics, never authority: capabilities are still the host-issued
//! resources.
pub use clay_collection::Batch;
pub use clay_collection::Query;
pub use clay_collection::RowChange;
pub use clay_collection::View;
pub use clay_collection::{self as collection};

mod manifest;
pub use manifest::ConfigGrant;
pub use manifest::Contribution;
pub use manifest::FacetAccess;
pub use manifest::MicroId;
pub use manifest::MicroManifest;
pub use manifest::Provision;
pub use manifest::ProvisionRef;

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
pub use bindings::exports::townframe::dayframe::document_events;
#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::micro_lifecycle;
#[cfg(feature = "wit")]
pub use bindings::exports::townframe::dayframe::presentation_events;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::documents;
#[cfg(feature = "wit")]
pub use bindings::townframe::dayframe::presentation;

#[cfg(feature = "wit")]
pub mod mirror;
#[cfg(feature = "wit")]
pub use mirror::ConfigDocument;
#[cfg(feature = "wit")]
pub use mirror::FacetState;
#[cfg(feature = "wit")]
pub use mirror::MirrorSnapshot;
#[cfg(feature = "wit")]
pub use mirror::MirrorWatch;

#[cfg(feature = "in_process")]
pub mod native;

#[cfg(feature = "view")]
pub mod view;

#[cfg(feature = "wasmtime")]
pub mod wasm;

#[cfg(feature = "dioxus")]
pub mod guest;
#[cfg(feature = "dioxus")]
pub use guest::GuestDom;

#[cfg(feature = "dioxus")]
pub mod wire;

#[cfg(all(test, feature = "dioxus", not(target_arch = "wasm32")))]
mod tests;
