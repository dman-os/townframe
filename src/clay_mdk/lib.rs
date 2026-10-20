//! The Micro Development Kit (ADR 015 §5): the single authoring API every micro uses.
//! `bindings` carries the WIT-generated surface; `mirror` is the typed document-mirror
//! authoring surface over it (typed decoding, availability/freshness evidence, named config
//! bindings); `guest` (feature `dioxus`) is the Dioxus VirtualDom adapter. The MDK adds
//! ergonomics, never authority: capabilities are still the host-issued WIT resources.
#[cfg(feature = "wit")]
pub mod bindings {
    wit_bindgen::generate!({
        path: "../clay_wit/wit",
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

/// The in-process backend (feature `native`): the same authoring vocabulary backed by
/// clay_host/clay_data handles. Native micros depend on the MDK, never on the host or data
/// crates directly (ADR 015 §3 dependency direction).
#[cfg(feature = "native")]
pub mod native {
    // `DocumentService` is re-exported for composition-time mock seeding helpers (for example
    // micro_sample::bind_mock_defaults); the runtime authoring surface is the handles below.
    pub use clay_data::{
        DocumentRef, DocumentService, DocumentSnapshot, DocumentWatch, Facet, FacetRoute,
        Operation, OperationReceipt,
    };
    pub use clay_host::{
        AccessError, CollectionReader, CollectionWriter, ComponentRef, HostedCollection, Instance,
        RootMount,
    };
}

#[cfg(feature = "dioxus")]
pub mod guest;
#[cfg(feature = "dioxus")]
pub use guest::GuestDom;

#[cfg(feature = "dioxus")]
pub mod wire;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
